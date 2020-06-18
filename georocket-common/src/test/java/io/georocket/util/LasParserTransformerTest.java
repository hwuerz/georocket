package io.georocket.util;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;

/**
 * Test {@link LasParserTransformer}
 * @author Hendrik M. Wuerz
 */
@RunWith(VertxUnitRunner.class)
public class LasParserTransformerTest {

  String lazFile = LasParserTransformerTest.class.getClassLoader()
          .getResource("io/georocket/util/pointCloud-1001-points.laz")
          .getPath();

  String lasinfoFile = LasParserTransformerTest.class.getClassLoader()
          .getResource("io/georocket/util/pointCloud-1001-points.lasinfo.txt")
          .getPath();

  /**
   * Run the test on a Vert.x test context
   */
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  /**
   * Check the generated stream event.
   * This method will ensure that
   * - The "pos" attribute is set to zero
   * - There is a stream event
   * - There are no errors
   * @param observable The input data. (= The bytes of the LAS file)
   * @param context The current test context.
   * @param additionalChecks A handler to perform the assert checks on the received LasStreamEvent
   */
  private void checkParser(Observable<Buffer> observable, TestContext context, Handler<LasStreamEvent> additionalChecks) {
    Async async = context.async();
    observable
            .compose(new LasParserTransformer(rule.vertx(), "./"))
            .doOnNext(lasStreamEvent -> {
              context.assertEquals(0, lasStreamEvent.getPos());
              additionalChecks.handle(lasStreamEvent);
            })
            .last()
            .subscribe(r -> {
              async.complete();
            }, context::fail);
  }

  /**
   * Test if a simple LAZ buffer can be parsed.
   * @param context the test context
   */
  @Test
  public void parseSimple(TestContext context) {
    Buffer buffer = rule.vertx().fileSystem().readFileBlocking(lazFile);
    checkParser(Observable.just(buffer), context, (lasStreamEvent) -> {
      context.assertEquals(new Lasinfo(lasinfoFile), lasStreamEvent.getEvent());
    });
  }
  
  /**
   * Test if a simple LAZ buffer can be parsed when split into two parts.
   * @param context the test context
   */
  @Test
  public void parseTwoParts(TestContext context) {
    Buffer buffer = rule.vertx().fileSystem().readFileBlocking(lazFile);
    Buffer b1 = buffer.getBuffer(0, buffer.length() / 2);
    Buffer b2 = buffer.getBuffer(buffer.length() / 2, buffer.length());
    checkParser(Observable.just(b1, b2), context, (lasStreamEvent) -> {
      context.assertEquals(new Lasinfo(lasinfoFile), lasStreamEvent.getEvent());
    });
  }

  /**
   * Test if a simple LAZ buffer can be parsed when split into arbitrary parts.
   * @param context the test context
   */
  @Test
  public void parseManyParts(TestContext context) {
    Buffer buffer = rule.vertx().fileSystem().readFileBlocking(lazFile);
    Buffer[] buffers = new Buffer[buffer.length()];
    for (int i = 0; i < buffer.length(); i++) {
      buffers[i] = buffer.getBuffer(i, i + 1);
    }
    checkParser(Observable.from(buffers), context, (lasStreamEvent) -> {
      context.assertEquals(new Lasinfo(lasinfoFile), lasStreamEvent.getEvent());
    });
  }

  /**
   * An empty buffer should not lead to any events.
   * @param context the test context
   */
  @Test
  public void parseEmpty(TestContext context) {
    Async async = context.async();
    Observable.just(Buffer.buffer())
            .compose(new LasParserTransformer(rule.vertx(), "./"))
            .doOnNext(lasStreamEvent -> {
              context.fail("An empty buffer should not lead to any stream events.");
            })
            .doOnError(context::fail)
            .doOnCompleted(async::complete)
            .subscribe();
  }

  /**
   * Ensures the actual point to be withing in expected bounding box.
   * @param context The current test context.
   * @param expectedMin The min coordinates of the bounding box.
   * @param expectedMax The max coordinates of the bounding box.
   * @param actual The actual point that should be within the bounding boxx.
   */
  private void assertInMinMaxRange(TestContext context, double[] expectedMin, double[] expectedMax, double[] actual) {
    context.assertEquals(expectedMin.length, expectedMax.length);
    context.assertEquals(expectedMax.length, actual.length);
    for (int i = 0; i < actual.length; i++) {
      context.assertTrue(expectedMin[i] == actual[i] || expectedMax[i] == actual[i]);
    }
  }

  /**
   * Parse an incomplete LAS file. This should report a warning.
   * @param context the test context
   */
  @Test
  public void parseIncompleteButParsable(TestContext context) {
    Buffer buffer = rule.vertx().fileSystem().readFileBlocking(lazFile);
    Buffer b1 = buffer.getBuffer(0, buffer.length() / 2);
    checkParser(Observable.just(b1), context, (lasStreamEvent) -> {
      // There are warnings because the header information do not match the received points.
      context.assertTrue(lasStreamEvent.getEvent().getWarningCounter() > 0);

      Lasinfo correctLasinfo = new Lasinfo(lasinfoFile);
      // Hint: The following data is included in the las-header. The header is unchanged in this test.
      context.assertEquals(correctLasinfo.getMin3d()[0], lasStreamEvent.getEvent().getMin3d()[0]);
      context.assertEquals(correctLasinfo.getMin3d()[1], lasStreamEvent.getEvent().getMin3d()[1]);
      context.assertEquals(correctLasinfo.getMin3d()[2], lasStreamEvent.getEvent().getMin3d()[2]);
      context.assertEquals(correctLasinfo.getMax3d()[0], lasStreamEvent.getEvent().getMax3d()[0]);
      context.assertEquals(correctLasinfo.getMax3d()[1], lasStreamEvent.getEvent().getMax3d()[1]);
      context.assertEquals(correctLasinfo.getMax3d()[2], lasStreamEvent.getEvent().getMax3d()[2]);
      context.assertEquals(correctLasinfo.getNumberOfPoints(), lasStreamEvent.getEvent().getNumberOfPoints());
      context.assertEquals(correctLasinfo.getCrs(), lasStreamEvent.getEvent().getCrs());
      context.assertEquals(correctLasinfo.getClassification(), lasStreamEvent.getEvent().getClassification());
    });
  }

  /**
   * If the LAS header is corrupt, we will not get any results.
   * @param context the test context.
   */
  @Test
  public void parseIncomplete(TestContext context) {
    Buffer buffer = rule.vertx().fileSystem().readFileBlocking(lazFile);
    Buffer b1 = buffer.getBuffer(0, 10);

    Async async = context.async();
    Observable.just(b1)
            .compose(new LasParserTransformer(rule.vertx(), "./"))
            .doOnNext(lasStreamEvent -> {
              context.fail("An invalid buffer should not lead to any stream events.");
            })
            .doOnError(context::fail)
            .doOnCompleted(async::complete)
            .subscribe();
  }

}
