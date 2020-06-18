package io.georocket.index.las;

import io.georocket.index.CRSAware;
import io.georocket.index.generic.BoundingBoxIndexer;
import io.georocket.index.xml.LasIndexer;
import io.georocket.util.CompoundCRSDecoder;
import io.georocket.util.LasStreamEvent;
import io.georocket.util.Lasinfo;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.cs.AxisDirection;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

/**
 * Indexes bounding boxes of Las chunks
 * @author Hendrik M. Wuerz
 */
public class LasBoundingBoxIndexer extends BoundingBoxIndexer
    implements LasIndexer, CRSAware {
  private static final CoordinateReferenceSystem WGS84 = DefaultGeographicCRS.WGS84;

  /**
   * The string of the detected CRS
   */
  protected String crsStr;

  /**
   * The detected CRS or <code>null</code> if either there was no CRS in the
   * chunk or if it was invalid
   */
  protected CoordinateReferenceSystem crs;

  /**
   * True if x and y are flipped in {@link #crs}
   */
  protected boolean flippedCRS;

  /**
   * A transformation from {@link #crs} to WGS84. <code>null</code> if
   * {@link #crs} is also <code>null</code>.
   */
  protected MathTransform transform;

  @Override
  public void onEvent(LasStreamEvent event) {
    Lasinfo lasinfo = event.getEvent();
    handleSrsName(lasinfo.getCrs());

    // Transform coordinates to WGS84 and store them in the bounding box.
    try {
      double[] min = new double[2];
      transform.transform(lasinfo.getMin(), 0, min, 0, 1);
      addToBoundingBox(min[flippedCRS ? 1 : 0], min[flippedCRS ? 0 : 1]);

      double[] max = new double[2];
      transform.transform(lasinfo.getMax(), 0, max, 0, 1);
      addToBoundingBox(max[flippedCRS ? 1 : 0], max[flippedCRS ? 0 : 1]);
    } catch (TransformException e) {
      // ignore
    }

  }

  @Override
  public void setFallbackCRSString(String crsStr) {
    handleSrsName(crsStr);
  }

  /**
   * Check if x and y are flipped in the given CRS
   * @param crs the CRS
   * @return true if x and y are flipped, false otherwise
   */
  public static boolean isFlippedCRS(CoordinateReferenceSystem crs) {
    // TODO Duplicated code. See XMLBoundingBoxIndexer
    if (crs.getCoordinateSystem().getDimension() == 2) {
      AxisDirection direction = crs.getCoordinateSystem().getAxis(0).getDirection();
      if (direction.equals(AxisDirection.NORTH) ||
              direction.equals(AxisDirection.UP)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Parse and decode a CRS string
   * @param srsName the CRS string (may be null)
   */
  private void handleSrsName(String srsName) {
    // TODO Duplicated code. See XMLBoundingBoxIndexer
    if (srsName == null || srsName.isEmpty()) {
      return;
    }

    if (crsStr != null && crsStr.equals(srsName)) {
      // same string, no need to parse
      return;
    }

    try {
      CoordinateReferenceSystem crs;
      // decode string
      if (CompoundCRSDecoder.isCompound(srsName)) {
        crs = CompoundCRSDecoder.decode(srsName);
      } else {
        crs = CRS.decode(srsName);
      }

      // only keep horizontal CRS
      CoordinateReferenceSystem hcrs = CRS.getHorizontalCRS(crs);
      if (hcrs != null) {
        crs = hcrs;
      }

      // find transformation to WGS84
      MathTransform transform = CRS.findMathTransform(crs, WGS84, true);
      boolean flippedCRS = isFlippedCRS(crs);

      this.crsStr = srsName;
      this.crs = crs;
      this.transform = transform;
      this.flippedCRS = flippedCRS;
    } catch (FactoryException e) {
      // unknown CRS or no transformation available
    }
  }
}
