package io.georocket;

import io.georocket.constants.AddressConstants;
import io.georocket.constants.ConfigConstants;
import io.georocket.index.xml.XMLCRSIndexer;
import io.georocket.input.Splitter.Result;
import io.georocket.input.geojson.GeoJsonSplitter;
import io.georocket.input.xml.FirstLevelSplitter;
import io.georocket.input.xml.XMLSplitter;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.IndexMeta;
import io.georocket.storage.LasChunkMeta;
import io.georocket.storage.RxStore;
import io.georocket.storage.StoreFactory;
import io.georocket.tasks.ImportingTask;
import io.georocket.tasks.TaskError;
import io.georocket.util.JsonParserTransformer;
import io.georocket.util.Lastools;
import io.georocket.util.RxUtils;
import io.georocket.util.StringWindow;
import io.georocket.util.UTF8BomFilter;
import io.georocket.util.Window;
import io.georocket.util.XMLParserTransformer;
import io.georocket.util.io.RxGzipReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.core.file.AsyncFile;
import io.vertx.rxjava.core.file.FileSystem;
import io.vertx.rxjava.core.streams.Pump;
import io.vertx.rxjava.core.streams.ReadStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import rx.Completable;
import rx.Observable;
import rx.Single;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.georocket.util.MimeTypeUtils.belongsTo;

/**
 * Imports file in the background
 * @author Michel Kraemer
 */
public class ImporterVerticle extends AbstractVerticle {
  private static Logger log = LoggerFactory.getLogger(ImporterVerticle.class);

  private static final int MAX_RETRIES = 5;
  private static final int RETRY_INTERVAL = 1000;
  private static final int MAX_PARALLEL_IMPORTS = 1;
  private static final int MAX_PARALLEL_ADDS = 10;
  

  /**
   * If {@link ConfigConstants#IMPORT_POINT_CLOUD_CHUNK_SIZE} is not defined, this fallback chunk size will be used.
   */
  private static final int DEFAULT_IMPORT_POINT_CLOUD_CHUNK_SIZE = 100000;

  protected RxStore store;
  private String incoming;
  private boolean paused;
  private Set<AsyncFile> filesBeingImported = new HashSet<>();
  private Lastools lastools; // Only required for the processing of point clouds.

  @Override
  public void start() {
    log.info("Launching importer ...");

    store = new RxStore(StoreFactory.createStore(getVertx()));
    String storagePath = config().getString(ConfigConstants.STORAGE_FILE_PATH);
    incoming = storagePath + "/incoming";
    String georocketHome = vertx.getOrCreateContext().config().getString(ConfigConstants.HOME);
    lastools = new Lastools(vertx.getDelegate(), georocketHome);

    vertx.eventBus().<JsonObject>localConsumer(AddressConstants.IMPORTER_IMPORT)
      .toObservable()
      .onBackpressureBuffer() // unlimited buffer
      .flatMapCompletable(msg -> {
        // call onImport() but ignore errors. onImport() will handle errors for us.
        return onImport(msg).onErrorComplete();
      }, false, MAX_PARALLEL_IMPORTS)
      .subscribe(v -> {
        // ignore
      }, err -> {
        // This is bad. It will unsubscribe the consumer from the eventbus!
        // Should never happen anyhow. If it does, something else has
        // completely gone wrong.
        log.fatal("Could not import file", err);
      });

    vertx.eventBus().localConsumer(AddressConstants.IMPORTER_PAUSE, this::onPause);
  }

  /**
   * Get the path on the filesystem, where the incoming file described in the
   * passed message body is stored.
   * @param filename The filename of the incoming file.
   * @return The path of the incoming file on the local filesystem.
   */
  protected String getIncomingFilePath(String filename) {
    return incoming + "/" + filename;
  }

  /**
   * Receives a name of a file to import
   * @param msg the event bus message containing the filename
   * @return a Completable that will complete when the file has been imported
   */
  protected Completable onImport(Message<JsonObject> msg) {
    JsonObject body = msg.body();
    String filename = body.getString("filename");
    String filepath;
    try {
      filepath = getIncomingFilePath(filename);
    } catch (Exception e) {
      return Completable.error(e);
    }
    String layer = body.getString("layer", "/");
    String contentType = body.getString("contentType");
    String correlationId = body.getString("correlationId");
    String fallbackCRSString = body.getString("fallbackCRSString");
    String contentEncoding = body.getString("contentEncoding");

    // get tags
    JsonArray tagsArr = body.getJsonArray("tags");
    List<String> tags = tagsArr != null ? tagsArr.stream().flatMap(o -> o != null ?
        Stream.of(o.toString()) : Stream.of()).collect(Collectors.toList()) : null;

    // get properties
    JsonObject propertiesObj = body.getJsonObject("properties");
    Map<String, Object> properties = propertiesObj != null ? propertiesObj.getMap() : null;

    // generate timestamp for this import
    long timestamp = System.currentTimeMillis();

    log.info("Importing [" + correlationId + "] to layer '" + layer + "'");

    FileSystem fs = vertx.fileSystem();
    OpenOptions openOptions = new OpenOptions().setCreate(false).setWrite(false);
    return fs.rxOpen(filepath, openOptions)
      .flatMap(f -> {
        filesBeingImported.add(f);
        return importFile(contentType, f, correlationId, filename, timestamp,
            layer, tags, properties, fallbackCRSString, contentEncoding)
          .doAfterTerminate(() -> {
            // delete file from 'incoming' folder
            log.debug("Deleting " + filepath + " from incoming folder");
            filesBeingImported.remove(f);
            f.rxClose()
              .flatMap(v -> fs.rxDelete(filepath))
              .subscribe(v -> {}, err -> {
                log.error("Could not delete file from 'incoming' folder", err);
              });
          });
      })
      .doOnSuccess(chunkCount -> {
        long duration = System.currentTimeMillis() - timestamp;
        log.info("Finished importing [" + correlationId + "] with " + chunkCount +
            " chunks to layer '" + layer + "' after " + duration + " ms");
      })
      .doOnError(err -> {
        long duration = System.currentTimeMillis() - timestamp;
        log.error("Failed to import [" + correlationId + "] to layer '" +
            layer + "' after " + duration + " ms", err);
      })
      .toCompletable();
  }

  /**
   * Import a file from the given read stream into the store. Inspect the file's
   * content type and forward to the correct import method.
   * @param contentType the file's content type
   * @param f the file to import
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one (may be <code>null</code>)
   * @param contentEncoding the content encoding of the file to be
   * imported (e.g. "gzip"). May be <code>null</code>.
   * @return a single that will emit with the number if chunks imported
   * when the file has been imported
   */
  protected Single<Integer> importFile(String contentType, ReadStream<Buffer> f,
      String correlationId, String filename, long timestamp, String layer,
      List<String> tags, Map<String, Object> properties, String fallbackCRSString,
      String contentEncoding) {
    if ("gzip".equals(contentEncoding)) {
      log.debug("Importing file compressed with GZIP");
      f = new RxGzipReadStream(f);
    } else if (contentEncoding != null && !contentEncoding.isEmpty()) {
      log.warn("Unknown content encoding: `" + contentEncoding + "'. Trying anyway.");
    }

    // let the task verticle know that we're now importing
    ImportingTask startTask = new ImportingTask(correlationId);
    startTask.setStartTime(Instant.now());
    vertx.eventBus().publish(AddressConstants.TASK_INC, JsonObject.mapFrom(startTask));

    Observable<Integer> result;
    if (belongsTo(contentType, "application", "xml") ||
        belongsTo(contentType, "text", "xml")) {
      result = importXML(f, correlationId, filename, timestamp, layer, tags,
        properties, fallbackCRSString);
    } else if (belongsTo(contentType, "application", "json")) {
      result = importJSON(f, correlationId, filename, timestamp, layer, tags, properties);
    } else if (belongsTo(contentType, "application", "vnd.las")) {
      result = importLas(f, correlationId, filename, timestamp, layer, tags, properties);
    } else {
      result = Observable.error(new NoStackTraceThrowable(String.format(
          "Received an unexpected content type '%s' while trying to import "
          + "file '%s'", contentType, filename)));
    }

    Consumer<Throwable> onFinish = t -> {
      // let the task verticle know that the import process has finished
      ImportingTask endTask = new ImportingTask(correlationId);
      endTask.setEndTime(Instant.now());
      if (t != null) {
        endTask.addError(new TaskError(t));
      }
      vertx.eventBus().publish(AddressConstants.TASK_INC,
          JsonObject.mapFrom(endTask));
    };

    return result.window(100)
      .flatMap(Observable::count)
      .doOnNext(n -> {
        // let the task verticle know that we imported n chunks
        ImportingTask currentTask = new ImportingTask(correlationId);
        currentTask.setImportedChunks(n);
        vertx.eventBus().publish(AddressConstants.TASK_INC,
            JsonObject.mapFrom(currentTask));
      })
      .reduce(0, (a, b) -> a + b)
      .toSingle()
      .doOnError(onFinish::accept)
      .doOnSuccess(i -> onFinish.accept(null));
  }

  /**
   * Imports an XML file from the given input stream into the store
   * @param f the XML file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @param fallbackCRSString the CRS which should be used if the imported
   * file does not specify one (may be <code>null</code>)
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  protected Observable<Integer> importXML(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags,
      Map<String, Object> properties, String fallbackCRSString) {
    UTF8BomFilter bomFilter = new UTF8BomFilter();
    Window window = new Window();
    XMLSplitter splitter = new FirstLevelSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    XMLCRSIndexer crsIndexer = new XMLCRSIndexer();
    return f.toObservable()
        .map(Buffer::getDelegate)
        .map(bomFilter::filter)
        .doOnNext(window::append)
        .compose(new XMLParserTransformer())
        .doOnNext(e -> {
          // save the first CRS found in the file
          if (crsIndexer.getCRS() == null) {
            crsIndexer.onEvent(e);
          }
        })
        .flatMap(splitter::onEventObservable)
        .flatMapSingle(result -> {
          String crsString = fallbackCRSString;
          if (crsIndexer.getCRS() != null) {
            crsString = crsIndexer.getCRS();
          }
          IndexMeta indexMeta = new IndexMeta(correlationId, filename,
              timestamp, tags, properties, crsString);
          return addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1);
        }, false, MAX_PARALLEL_ADDS);
  }

  /**
   * Imports a JSON file from the given input stream into the store
   * @param f the JSON file to read
   * @param correlationId a unique identifier for this import process
   * @param filename the name of the file currently being imported
   * @param timestamp denotes when the import process has started
   * @param layer the layer where the file should be stored (may be null)
   * @param tags the list of tags to attach to the file (may be null)
   * @param properties the map of properties to attach to the file (may be null)
   * @return an observable that will emit the number 1 when a chunk has been imported
   */
  protected Observable<Integer> importJSON(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags, Map<String, Object> properties) {
    UTF8BomFilter bomFilter = new UTF8BomFilter();
    StringWindow window = new StringWindow();
    GeoJsonSplitter splitter = new GeoJsonSplitter(window);
    AtomicInteger processing = new AtomicInteger(0);
    return f.toObservable()
        .map(Buffer::getDelegate)
        .map(bomFilter::filter)
        .doOnNext(window::append)
        .compose(new JsonParserTransformer())
        .flatMap(splitter::onEventObservable)
        .flatMapSingle(result -> {
          IndexMeta indexMeta = new IndexMeta(correlationId, filename,
              timestamp, tags, properties, null);
          return addToStoreWithPause(result, layer, indexMeta, f, processing)
              .toSingleDefault(1);
        }, false, MAX_PARALLEL_ADDS);
  }

  /**
   * Import a *.las or *.laz file from the given input stream into the store.
   * Hint: laz is the lossless compression of las. Both can be handled.
   * All imported chunks are stored as base64 encoded laz files.
   * In a further version this might be changed to a binary format, but currently
   * the store can only handle strings.
   * @param f the JSON file to read.
   * @param correlationId a unique identifier for this import process.
   * @param filename the name of the file currently being imported.
   * @param timestamp denotes when the import process has started.
   * @param layer the layer where the file should be stored (may be null).
   * @param tags the list of tags to attach to the file (may be null).
   * @param properties the map of properties to attach to the file (may be null).
   * @return an observable that will emit the number 1 when a chunk has been imported.
   */
  protected Observable<Integer> importLas(ReadStream<Buffer> f, String correlationId,
      String filename, long timestamp, String layer, List<String> tags, Map<String, Object> properties) {

    Integer configChunkSize = config().getInteger(ConfigConstants.IMPORT_POINT_CLOUD_CHUNK_SIZE);
    int chunkSize = configChunkSize != null ? configChunkSize : DEFAULT_IMPORT_POINT_CLOUD_CHUNK_SIZE;

    FileSystem fs = vertx.fileSystem();
    try {
      // LASTools requires the las or laz extension. Otherwise the files are parsed as xyz.
      // The tmpFile contains the new input file in an uncompressed way.
      // It does not matter if the extension is las or laz. Lastools will handle it correctly in any case.
      String tmpFile = File.createTempFile(filename, ".las").getCanonicalPath();
      // The tmpDirectory contains all generated chunks based on the input file.
      String tmpDirectory = Files.createTempDirectory(filename).toFile().getCanonicalPath();
      OpenOptions openOptions = new OpenOptions().setCreate(true).setWrite(true);
      return fs.rxOpen(tmpFile, openOptions)
              .toObservable()
              .flatMap(file -> {
                // Write the new data (f) to the tmp file (file). This will decompress it if required.
                // Returns a pump observable for the coping.
                ObservableFuture<Void> pumpObservable = RxHelper.observableFuture();
                Handler<AsyncResult<Void>> pumpHandler = pumpObservable.toHandler();
                Pump.pump(f, file).start();
                Handler<Throwable> errHandler = (Throwable t) -> {
                  f.endHandler(null);
                  file.close();
                  pumpHandler.handle(Future.failedFuture(t));
                };
                file.exceptionHandler(errHandler);
                f.exceptionHandler(errHandler);
                f.endHandler(v -> file.close(v2 -> pumpHandler.handle(Future.succeededFuture())));
                f.resume();
                return pumpObservable;
              })
              .flatMap(v -> {
                // Apply the LASTools to the new file. Split it in chunks. Store the chunks in tmpDirectory.
                log.debug("Apply LASTools to the tmpFile " + tmpFile);
                ObservableFuture<Void> observable = RxHelper.observableFuture();
                Handler<AsyncResult<Void>> handler = observable.toHandler();
                lastools.lasmerge(tmpFile, tmpDirectory, chunkSize, handler);
                return observable;
              })
              .flatMap(v -> {
                // Get paths to the generated chunks.
                File[] chunkFiles = new File(tmpDirectory).listFiles();
                if (chunkFiles == null) {
                  log.warn("No chunks created for " + filename);
                  return Observable.empty();
                }
                log.info(chunkFiles.length + " chunks created for " + filename + " in " + tmpDirectory);
                return Observable.from(chunkFiles);
              })
              .flatMapSingle(chunkFile -> {
                // Store each chunk.
                try {
                  // The store can only handle string values.
                  // We have to store the base64 encoding instead of the raw binary.
                  // In a further version, the store should be extended ti support binary data.
                  byte[] encoded = Base64.encodeBase64(FileUtils.readFileToByteArray(chunkFile));
                  String chunk = new String(encoded, StandardCharsets.US_ASCII);

                  IndexMeta indexMeta = new IndexMeta(correlationId, filename,
                          timestamp, tags, properties, null);
                  // Lastools creates the chunk files with an ascending number as filename.
                  // We can use the name to identify the order of the chunk in the original file.
                  String chunkNumberAsString = FilenameUtils.removeExtension(chunkFile.getName());
                  int chunkNumber = Integer.parseInt(chunkNumberAsString);
                  ChunkMeta meta = new LasChunkMeta(chunkNumber);

                  return addToStore(chunk, meta, layer, indexMeta)
                          .andThen(Completable.defer(() -> {
                            chunkFile.deleteOnExit(); // Clean up the local chunk file. It is in the store now.
                            return Completable.complete();
                          }))
                          .toSingleDefault(1);
                } catch (IOException e) {
                  log.error("Could not store chunk", e);
                  return Single.error(e);
                }
              })
              .doOnCompleted(() -> {
                // Clean up: Delete tmp file and chunk directory.
                try {
                  new File(tmpFile).delete(); // The input file. Resulted from writing the ReadStream.
                  FileUtils.deleteDirectory(new File(tmpDirectory)); // The chunk directory.
                } catch (IOException e) {
                  log.warn("The chunk tmp directory could not be deleted.", e);
                }
              });

    } catch (IOException e) {
      log.error("Can not create tmp file for LAS input", e);
      return Observable.error(e);
    }
  }

  /**
   * Handle a pause message
   * @param msg the message
   */
  private void onPause(Message<Boolean> msg) {
    Boolean paused = msg.body();
    if (paused == null || !paused) {
      if (this.paused) {
        log.info("Resuming import");
        this.paused = false;
        for (AsyncFile f : filesBeingImported) {
          f.resume();
        }
      }
    } else {
      if (!this.paused) {
        log.info("Pausing import");
        this.paused = true;
        for (AsyncFile f : filesBeingImported) {
          f.pause();
        }
      }
    }
  }

  /**
   * Add a chunk to the store. Pause the given read stream before adding and
   * increase the given counter. Decrease the counter after the chunk has been
   * written and only resume the read stream if the counter is <code>0</code>.
   * This is necessary because the writing to the store may take longer than
   * reading. We need to pause reading so the store is not overloaded (i.e.
   * we handle back-pressure here).
   * @param chunk the chunk to write
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @param f the read stream to pause while writing
   * @param processing an AtomicInteger keeping the number of chunks currently
   * being written (should be initialized to <code>0</code> the first time this
   * method is called)
   * @return a Completable that will complete when the operation has finished
   */
  private Completable addToStoreWithPause(Result<? extends ChunkMeta> chunk,
      String layer, IndexMeta indexMeta, ReadStream<Buffer> f, AtomicInteger processing) {
    // pause stream while chunk is being written
    f.pause();

    // count number of chunks being written
    processing.incrementAndGet();

    return addToStore(chunk.getChunk(), chunk.getMeta(), layer, indexMeta)
        .doOnCompleted(() -> {
          // resume stream only after all chunks from the current
          // buffer have been stored
          if (processing.decrementAndGet() == 0 && !paused) {
            f.resume();
          }
        });
  }

  /**
   * Add a chunk to the store. Retry operation several times before failing.
   * @param chunk the chunk to add
   * @param meta the chunk's metadata
   * @param layer the layer the chunk should be added to (may be null)
   * @param indexMeta metadata specifying how the chunk should be indexed
   * @return a Completable that will complete when the operation has finished
   */
  protected Completable addToStore(String chunk, ChunkMeta meta,
      String layer, IndexMeta indexMeta) {
    return Completable.defer(() -> store.rxAdd(chunk, meta, layer, indexMeta))
        .retryWhen(RxUtils.makeRetry(MAX_RETRIES, RETRY_INTERVAL, log));
  }
}
