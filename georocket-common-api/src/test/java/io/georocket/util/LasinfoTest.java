package io.georocket.util;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test {@link Lasinfo}
 * @author Hendrik M. Wuerz
 */
@RunWith(VertxUnitRunner.class)
public class LasinfoTest {

  /**
   * The lasinfo output is parsed correctly.
   */
  @Test
  public void lasinfoReadTest(TestContext context) {
    String lasinfoExampleOutputFile = Lasinfo.class.getClassLoader()
            .getResource("io/georocket/util/pointCloud-1001-points.lasinfo.txt")
            .getPath();

    Lasinfo result = new Lasinfo(lasinfoExampleOutputFile);
    context.assertEquals(360369.96, result.getMin3d()[0]);
    context.assertEquals(360378.33, result.getMax3d()[0]);
    context.assertEquals(5613040.02, result.getMin3d()[1]);
    context.assertEquals(5613059.99, result.getMax3d()[1]);
    context.assertEquals(164.68, result.getMin3d()[2]);
    context.assertEquals(165.24, result.getMax3d()[2]);
    context.assertEquals("EPSG:25832", result.getCrs());
    context.assertEquals(0, result.getWarningCounter());
    context.assertEquals(1001, result.getNumberOfPoints());
    context.assertEquals(1, result.getClassification().size());
    context.assertTrue(result.getClassification().contains(2));
  }
}
