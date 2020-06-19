package io.georocket.util;

import io.georocket.util.io.DelegateBase64ReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rx.java.ReadStreamSubscriber;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;

import java.util.Base64;
import java.util.function.Function;

/**
 * Test {@link DelegateBase64ReadStream}
 * @author Hendrik M. Wuerz
 */
@RunWith(VertxUnitRunner.class)
public class DelegateBase64ReadStreamTest {

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    /**
     * Encodes the passed input string and ensures a correct decoding.
     * @param context The current test context.
     * @param input The string to be tested.
     */
    private void test(TestContext context, String input) {
        byte[] encoded = Base64.getEncoder().encode(input.getBytes());

        Buffer[] buffers = new Buffer[encoded.length];
        for (int i = 0; i < encoded.length; i++) {
            buffers[i] = Buffer.buffer().appendByte(encoded[i]);
        }

        test(context, input, buffers);
    }

    /**
     * Pass the buffers to the read stream and ensures the response
     * to be equal to the passed input string.
     * @param context The current test context.
     * @param input The original non-encoded input string.
     * @param buffers The buffers that contain the base64 encoded data.
     */
    private void test(TestContext context, String input, Buffer[] buffers) {
        Async async = context.async();

        ReadStream<Buffer> readStream = ReadStreamSubscriber.asReadStream(Observable.from(buffers), Function.identity());
        readStream = new DelegateBase64ReadStream(readStream);

        Buffer decoded = Buffer.buffer();
        readStream
                .endHandler(v -> {
                    context.assertEquals(new String(decoded.getBytes()), input);
                    async.complete();
                })
                .exceptionHandler(context::fail)
                .handler(decoded::appendBuffer);
    }

    @Test
    public void normalString(TestContext context) {
        String input = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua";
        test(context, input);
    }

    @Test
    public void OneChar(TestContext context) {
        test(context, "A");
    }

    @Test
    public void TwoChars(TestContext context) {
        test(context, "AB");
    }

    @Test
    public void ThreeChars(TestContext context) {
        test(context, "ABC");
    }

    @Test
    public void FourChars(TestContext context) {
        test(context, "ABCD");
    }

    @Test
    public void bigSplits(TestContext context) {
        String input = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua";

        byte[] encoded = Base64.getEncoder().encode(input.getBytes());
        Buffer encodedBuffer = Buffer.buffer(encoded);

        Buffer[] buffers = new Buffer[]{
                encodedBuffer.getBuffer(0, 15), // There are 3 bytes to much for the complete processing.
                encodedBuffer.getBuffer(15, 20), // Together with the 3 bytes from above, this fits exactly.
                encodedBuffer.getBuffer(20, encodedBuffer.length()) // Just get the rest.
        };

        test(context, input, buffers);
    }

    @Test
    public void invalidBase64InvalidBytes(TestContext context) {
        Async async = context.async();
        String input = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua";

        byte[] encoded = Base64.getEncoder().encode(input.getBytes());
        encoded[0] = 0x40; // This is the ACII code of '@'. It is not a valid base64 character.
        Buffer encodedBuffer = Buffer.buffer(encoded);

        ReadStream<Buffer> readStream = ReadStreamSubscriber.asReadStream(Observable.from(new Buffer[]{encodedBuffer}), Function.identity());
        readStream = new DelegateBase64ReadStream(readStream);

        Buffer decoded = Buffer.buffer();
        readStream
                .endHandler(v -> {
                    if (async.count() > 0) {
                        context.fail("An invalid base64 string should not lead to a valid output.");
                    }
                })
                .exceptionHandler(error -> {
                    if (async.count() > 0) {
                        async.complete();
                    }
                })
                .handler(decoded::appendBuffer);
    }

    @Test
    public void invalidBase64MissingBytes(TestContext context) {
        Async async = context.async();
        String input = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua";

        byte[] encoded = Base64.getEncoder().encode(input.getBytes());
        // Only 15 bytes. 15 % 4 == 3 != 0 --> Not a valid base64 string.
        Buffer encodedBuffer = Buffer.buffer(encoded).getBuffer(0, 15);

        ReadStream<Buffer> readStream = ReadStreamSubscriber.asReadStream(Observable.from(new Buffer[]{encodedBuffer}), Function.identity());
        readStream = new DelegateBase64ReadStream(readStream);

        Buffer decoded = Buffer.buffer();
        readStream
                .endHandler(v -> {
                    if (async.count() > 0) {
                        context.fail("An invalid base64 string should not lead to a valid output.");
                    }
                })
                .exceptionHandler(error -> {
                    if (async.count() > 0) {
                        async.complete();
                    }
                })
                .handler(decoded::appendBuffer);
    }
}
