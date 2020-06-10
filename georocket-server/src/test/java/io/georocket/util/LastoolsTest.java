package io.georocket.util;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.stream.Collectors;

/**
 * Test {@link Lastools}
 * @author Hendrik M. Wuerz
 */
@RunWith(VertxUnitRunner.class)
public class LastoolsTest {

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    /**
     * Set up the unit tests
     */
    @Before
    public void setUp() {
        rule.vertx()
                .getOrCreateContext()
                .config()
                .put(ConfigConstants.HOME, "./");
    }

    /**
     * The lasinfo output is parsed correctly.
     */
    @Test
    public void lasinfoReadTest(TestContext context) {
        URL lasinfoExampleOutputFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.meta.txt");

        Lastools.Lasinfo result = new Lastools.Lasinfo(lasinfoExampleOutputFile.getPath());
        context.assertEquals(36036996.0, result.minX.get());
        context.assertEquals(36037833.0, result.maxX.get());
        context.assertEquals(61304002.0, result.minY.get());
        context.assertEquals(61305999.0, result.maxY.get());
        context.assertEquals(16468.0, result.minZ.get());
        context.assertEquals(16524.0, result.maxZ.get());
        context.assertEquals("ETRS89 / UTM 32N", result.crs.get());
        context.assertEquals(1, result.classification.size());
        context.assertTrue(result.classification.contains(2));
    }

    /**
     * Get the lasinfo response and check it to be correct.
     */
    @Test
    public void lasinfoTest(TestContext context) {
        Async async = context.async();
        Lastools lastools = new Lastools(rule.vertx());

        URL lazFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.laz");
        lastools.lasinfo(lazFile.getPath(), asyncResult -> {
            if (asyncResult.failed()) {
                context.fail(asyncResult.cause());
                async.complete();
                return;
            }
            Lastools.Lasinfo result = asyncResult.result();
            context.assertEquals(36036996.0, result.minX.get());
            context.assertEquals(36037833.0, result.maxX.get());
            context.assertEquals(61304002.0, result.minY.get());
            context.assertEquals(61305999.0, result.maxY.get());
            context.assertEquals(16468.0, result.minZ.get());
            context.assertEquals(16524.0, result.maxZ.get());
            context.assertEquals("ETRS89 / UTM 32N", result.crs.get());
            context.assertEquals(1, result.classification.size());

            async.complete();
        });

    }
}
