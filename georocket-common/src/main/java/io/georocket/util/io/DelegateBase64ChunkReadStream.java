package io.georocket.util.io;

import io.georocket.storage.ChunkReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A simple read stream for base64 chunks.
 * Maps them to binary.
 * Wraps around another chunk read stream that doesn't need to be closed or is closed by the caller.
 * @author Hendrik M. Wuerz
 */
public class DelegateBase64ChunkReadStream extends DelegateBase64ReadStream implements ChunkReadStream {
  private final long size;

  /**
   * Constructs a new read stream
   * @param delegate the underlying chunk read stream
   */
  public DelegateBase64ChunkReadStream(ChunkReadStream delegate) {
    super(delegate);
    if (delegate.getSize() % 4 != 0) {
      throw new RuntimeException("The chunk read stream is not a base64 stream. Its length is " +
              delegate.getSize() + ". The length of a base64 string is a multiple of 4.");
    }
    this.size = (delegate.getSize() / 4) * 3;
  }
  
  @Override
  public long getSize() {
    return size;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    if (handler != null) {
      handler.handle(Future.succeededFuture());
    }
  }
}
