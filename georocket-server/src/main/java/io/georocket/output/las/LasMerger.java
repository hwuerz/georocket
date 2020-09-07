package io.georocket.output.las;

import io.georocket.constants.ConfigConstants;
import io.georocket.util.Lastools;
import io.georocket.output.Merger;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.LasChunkMeta;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import org.apache.commons.codec.binary.Base64;
import rx.Completable;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

/**
 * Merges chunks to valid LAS documents.
 * @author Hendrik M. Wuerz
 */
public class LasMerger implements Merger<LasChunkMeta> {
  private static final Logger log = LoggerFactory.getLogger(LasMerger.class);

  private final Vertx vertx;

  /**
   * The directory in that the chunks should be stored until the request is finished.
   */
  private final String chunkDirectory;

  /**
   * A list of paths to chunk point clouds.
   * These files are already decoded from base64 to binary.
   */
  private final List<String> chunkFiles = new LinkedList<>();

  /**
   * The LAStools wrapper used to merge the chunks.
   */
  private final Lastools lastools;

  /**
   * Create a new merger.
   */
  public LasMerger(Vertx vertx) {
      this.vertx = vertx;
      chunkDirectory = vertx.fileSystem().createTempDirectoryBlocking("chunksToMerge", "rwxrwxrwx");
      String georocketHome = vertx.getOrCreateContext().config().getString(ConfigConstants.HOME);
      lastools = new Lastools(vertx, georocketHome);
  }

  @Override
  public Completable init(LasChunkMeta meta) {
    return Completable.complete();
  }

  @Override
  public Completable merge(ChunkReadStream chunk, LasChunkMeta meta, WriteStream<Buffer> out) {

    ObservableFuture<Void> observableFuture = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = observableFuture.toHandler();

    // The binary las chunks are stored as base64 strings in the store.
    // Get the total chunk and decode it to binary.
    // Write the binary data to a file. (Will be merged in finish() )
    Buffer base64Chunk = Buffer.buffer();
    chunk.handler(base64Chunk::appendBuffer)
            .exceptionHandler(cause -> {
              chunk.endHandler(null);
              handler.handle(Future.failedFuture(cause));
            })
            .endHandler(v -> {
              String base64ChunkString = base64Chunk.toString(StandardCharsets.US_ASCII);
              byte[] decoded = Base64.decodeBase64(base64ChunkString);
              vertx.fileSystem().createTempFile(chunkDirectory, "mergedChunk", ".laz", "rwxr-x---", tmpFile -> {
                if (tmpFile.succeeded()) {
                  try {
                    Files.write(new File(tmpFile.result()).toPath(), decoded);
                    chunkFiles.add(tmpFile.result());
                    log.info("Received a chunk to the GET response");
                    handler.handle(Future.succeededFuture());
                  } catch (IOException e) {
                    log.error("Could not write LAS chunk to local file", e);
                    handler.handle(Future.failedFuture(e));
                  }
                } else {
                  log.error("Could not create chunk tmp file.", tmpFile.cause());
                  handler.handle(Future.failedFuture(tmpFile.cause()));
                }
              });
            });

//    out.write(Buffer.buffer("x"), dummyHandler -> {
//      if (dummyHandler.failed()) {
//        log.error("x could not be written ", dummyHandler.cause());
//      } else {
//        log.info("Wrote x");
//      }
//    });
    return observableFuture.toCompletable();
  }

  @Override
  public Completable finish(WriteStream<Buffer> out) {

    ObservableFuture<Void> observableFuture = RxHelper.observableFuture();
    Handler<AsyncResult<Void>> handler = observableFuture.toHandler();
    try {
      // Generate a list of all chunk files. It will be passed to lastools for merge.
      String listOfFiles = vertx.fileSystem().createTempFileBlocking("lof", ".txt");
      String chunkFileList = String.join("\n", chunkFiles);
      vertx.fileSystem().writeFileBlocking(listOfFiles, Buffer.buffer(chunkFileList));

      log.info("Got all chunks for the GET request. " + chunkFiles.size() + " in total. Start merging.");

      Path outputFile = Files.createTempFile("mergedChunk", ".laz");
      lastools.lasmerge(listOfFiles, outputFile.toAbsolutePath().toString(), result -> {
        if (result.succeeded()) {
          log.info("Merging finished. Deliver file. " + outputFile.toString());

          vertx.fileSystem().open(outputFile.toString(), new OpenOptions().setDeleteOnClose(true), asyncResult -> {
            if (asyncResult.succeeded()) {
              ReadStream<Buffer> responseData = asyncResult.result();
//              try {
//                Thread.sleep(30*1000);
//              } catch (InterruptedException e) {
//                e.printStackTrace();
//              }
//              out.exceptionHandler(outErrorHandler -> {
//                log.error("Could not write data to response", outErrorHandler);
//              });
//              out.write(Buffer.buffer("Kommt das durch???"));
              responseData
                      // Write the merged point cloud to the `out`-WriteStream.
                      // Hint: We cannot use pipe because `out` must not be closed when response data finishes.
                      .handler(buffer -> {
                        log.info("Write " + buffer.length() + " bytes.");
                        out.write(buffer);
                        if (out.writeQueueFull()) {
                          log.info("Paused output");
                          responseData.pause();
                          out.drainHandler(v -> {
                            log.info("Resumed output");
                            responseData.resume();
                          });
                        }
                      })
                      .exceptionHandler(cause -> {
                        log.error("Reading the merged chunk file failed", cause);
                        vertx.fileSystem().deleteRecursive(chunkDirectory, true, handlerChunkDirectoryDelete -> {
                          vertx.fileSystem().delete(listOfFiles, handlerListOfFilesDelete -> {
                            handler.handle(Future.failedFuture(cause));
                          });
                        });
                      })
                      .endHandler(v -> {
//                        vertx.fileSystem().deleteRecursive(chunkDirectory, true, handlerChunkDirectoryDelete -> {
//                          vertx.fileSystem().delete(listOfFiles, handlerListOfFilesDelete -> {
                            handler.handle(Future.succeededFuture());
//                          });
//                        });
                      });
            } else {
              log.error("Could not open the file with the merged chunks.", new RuntimeException(asyncResult.cause()));
              handler.handle(Future.failedFuture(asyncResult.cause()));
            }
          });
        } else {
          log.error("Could not merge the chunks via the command line application LASTools.", new RuntimeException(result.cause()));
          handler.handle(Future.failedFuture(result.cause()));
        }
      });
    } catch (IOException e) {
      log.error("Could not create a tmp file to the merged chunks.", e);
      handler.handle(Future.failedFuture(e));
    }

    return observableFuture.toCompletable();
  }
}
