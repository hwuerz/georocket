package io.georocket.util.io;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

import java.util.Base64;

/**
 * Decodes a base64 string on the fly.
 * Wraps around another read stream with base64 encoded data.
 * Converts these data to binary.
 * @author Hendrik M. Wuerz
 */
public class DelegateBase64ReadStream extends DelegateReadStream<Buffer> {

    public static class InvalidBase64 extends RuntimeException {
        public InvalidBase64(String s) {
            super(s);
        }
    }

    /**
     * A local cache for incoming data.
     * Sometimes, the incoming data can not be converted completely.
     * In this case the remaining data is stored in this cache until the
     * next part of incoming data is available.
     */
    private Buffer cache = Buffer.buffer();

    /**
     * The registered exception handler if there is any.
     * Might be null.
     */
    private Handler<Throwable> exceptionHandler;

    /**
     * Constructs a new ReadStream that decodes a base64 encoded ReadStream on the fly.
     *
     * @param delegate the stream to delegate to
     */
    public DelegateBase64ReadStream(ReadStream<Buffer> delegate) {
        super(delegate);
    }

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        super.handler(base64Buffer -> {
            cache.appendBuffer(base64Buffer);
            if (cache.length() < 4) { // Base64 strings use 4 bytes to encode 3 bytes in the original input.
                handler.handle(Buffer.buffer());
            } else {
                int end = cache.length() - cache.length() % 4;
                Buffer toDecode = cache.getBuffer(0, end);
                byte[] decoded = Base64.getDecoder().decode(toDecode.getBytes());
                cache = cache.getBuffer(end, cache.length());
                handler.handle(Buffer.buffer(decoded));
            }
        });
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        super.endHandler(v -> {
            if (cache.length() > 0) {
                // We have to end the stream, but the cache is not empty.
                // --> The incoming base64 stream was incomplete.
                InvalidBase64 error = new InvalidBase64("Invalid base64 input. Could not process all bytes. Possible data loss.");
                if (exceptionHandler != null) {
                    exceptionHandler.handle(error);
                } else {
                    throw error;
                }
            }
            endHandler.handle(v);
        });
        return this;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        super.exceptionHandler(handler);
        return this;
    }
}
