package io.georocket.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.ReadStreamSubscriber;
import io.vertx.rx.java.RxHelper;
import rx.Observable;
import rx.Observable.Transformer;

import java.util.function.Function;

/**
 * A reusable transformer that you can apply to an RxJava {@link Observable}
 * using {@link Observable#compose(Transformer)}. It transforms
 * {@link Buffer}s into {@link StreamEvent}s.
 * @author Hendrik M. Wuerz
 */
public class LasParserTransformer implements Transformer<Buffer, LasStreamEvent> {

  /**
   * The wrapper around the lastools.
   * It is only create once in the constructor.
   */
  private final Lastools lastools;

  /**
   * The path to the temporary file used to store the chunk.
   */
  private final String chunkFilePath;

  /**
   * The open file defined by chunkFilePath.
   */
  private final AsyncFile chunkFile;

  /**
   * Create a new LasParserTransformer
   * @param vertx A vertx instance used to access the filesystem.
   * @param georocketHome The georocket home directory as defined in the config.
   */
  public LasParserTransformer(Vertx vertx, String georocketHome) {
    lastools = new Lastools(vertx, georocketHome);
    chunkFilePath = vertx.fileSystem().createTempFileBlocking("chunk", ".laz");
    System.out.println("Create a new chunk tmp file at " + chunkFilePath);
    OpenOptions openOptions = new OpenOptions().setWrite(true);
    chunkFile = vertx.fileSystem().openBlocking(chunkFilePath, openOptions);
  }

  @Override
  public Observable<LasStreamEvent> call(Observable<Buffer> o) {
    // Map input to a read stream.
    ReadStream<Buffer> bufferReadStream = ReadStreamSubscriber.asReadStream(o, Function.identity());

    // Create output observable.
    ObservableFuture<LasStreamEvent> observableFuture = RxHelper.observableFuture();
    Handler<AsyncResult<LasStreamEvent>> handler = observableFuture.toHandler();

    bufferReadStream.endHandler(v -> { // As soon as there are no more data available...
        chunkFile.end(writeEndHandler -> { // ... close the chunk file. When it is closed ...
            lastools.lasinfo(chunkFilePath, lasInfoHandler -> { // ... inform lastools to get the infos.
                handler.handle(lasInfoHandler.map(LasStreamEvent::new));
            });
        });
    });

    Pump.pump(bufferReadStream, chunkFile).start();

    // If there are any errors, just send an empty observable.
    return observableFuture.onErrorResumeNext(Observable.empty());
  }

}
