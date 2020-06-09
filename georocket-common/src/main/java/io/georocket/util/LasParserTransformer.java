package io.georocket.util;

import io.vertx.core.buffer.Buffer;
import rx.Observable;
import rx.Observable.Transformer;

import java.util.Iterator;

/**
 * A reusable transformer that you can apply to an RxJava {@link Observable}
 * using {@link Observable#compose(Transformer)}. It transforms
 * {@link Buffer}s into {@link StreamEvent}s.
 * @author Hendrik M. Wuerz
 */
public class LasParserTransformer implements Transformer<Buffer, StreamEvent> {

  @Override
  public Observable<StreamEvent> call(Observable<Buffer> o) {
    return o
      .flatMap(buf -> Observable.from(() -> new StreamEventIterator(buf)))
      .concatWith(Observable.from(() -> new StreamEventIterator()));
  }

  private class StreamEventIterator implements Iterator<StreamEvent> {

    /**
     * True if the end of file has been reached
     */
    private boolean eof = false;

    /**
     * Create a new iterator that does not feed any input but just processes
     * remaining events
     */
    public StreamEventIterator() {
    }

    /**
     * Create a new iterator and iterates over generated events.
     * @param buf the buffer to feed
     */
    public StreamEventIterator(Buffer buf) {
    }

    @Override
    public boolean hasNext() {
        return !eof;
    }

    @Override
    public StreamEvent next() {
      eof = true;
      return new StreamEvent(0);
    }
  }
}
