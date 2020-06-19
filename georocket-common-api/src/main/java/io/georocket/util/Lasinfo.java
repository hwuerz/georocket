package io.georocket.util;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java Object to store the las meta infos that are extracted from lasinfo.
 */
public class Lasinfo {
  private static final Logger log = LoggerFactory.getLogger(Lasinfo.class);

  /**
   * Helper class to pass a primitive value by reference to other methods.
   * @param <T> The type of the stored value.
   */
  private static class MutableWrapper<T> {
    private T value = null;
    private final String contentHint;
    private MutableWrapper(String contentHint) {
      this.contentHint = contentHint;
    }
    public T get() {
      return value;
    }
    private void set(T newValue) {
      if (value != null) {
        log.warn("Overwrite " + contentHint + " Old value: " + value + " New value: " + newValue);
      }
      value = newValue;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      MutableWrapper<?> mutableWrapper = (MutableWrapper<?>)obj;
      if (value == null && mutableWrapper.value == null) return true;
      if (value == null) return false;
      return value.equals(mutableWrapper.value);
    }
  }

  // All available meta infos.
  private final MutableWrapper<Double>
          minX = new MutableWrapper<>("minX"),
          minY = new MutableWrapper<>("minY"),
          minZ = new MutableWrapper<>("minZ"),
          maxX = new MutableWrapper<>("maxX"),
          maxY = new MutableWrapper<>("maxY"),
          maxZ = new MutableWrapper<>("maxZ");
  private final MutableWrapper<String> crs = new MutableWrapper<>("crs");
  private final MutableWrapper<Integer> warningCounter = new MutableWrapper<>("warning counter");
  private final MutableWrapper<Integer> numberOfPoints = new MutableWrapper<>("number of points");
  private final HashSet<Integer> classification = new HashSet<>();

  public double[] getMin() {
    return new double[]{ minX.get(), minY.get() };
  }

  public double[] getMin3d() {
    return new double[]{ minX.get(), minY.get(), minZ.get() };
  }

  public double[] getMax() {
    return new double[]{ maxX.get(), maxY.get() };
  }

  public double[] getMax3d() {
    return new double[]{ maxX.get(), maxY.get(), maxZ.get() };
  }

  public String getCrs() {
    return crs.get();
  }

  public HashSet<Integer> getClassification() {
    return classification;
  }

  public int getWarningCounter() {
    return warningCounter.get() != null ? warningCounter.get() : 0;
  }

  public int getNumberOfPoints() {
    return numberOfPoints.get();
  }

  /**
   * Copy the meta infos from a lasinfo output file to this object.
   * @param pathToInfoFile The path to the lasinfo output file.
   */
  public Lasinfo(String pathToInfoFile) {
    // Regex patterns. They are applied on each line. If they match, the infos are copied.
    Pattern boundingBoxMinPattern = Pattern.compile("^\\s+min x y z: \\s+(\\d+\\.?\\d*) (\\d+\\.?\\d*) (\\d+\\.?\\d*)$");
    Pattern boundingBoxMaxPattern = Pattern.compile("^\\s+max x y z: \\s+(\\d+\\.?\\d*) (\\d+\\.?\\d*) (\\d+\\.?\\d*)$");
    Pattern crsPattern = Pattern.compile("^\\s+.key 3072.*value_offset (\\d+) - ProjectedCSTypeGeoKey: (.+)$");
    Pattern warningPattern = Pattern.compile("^\\s*WARNING: (.+)$");
    Pattern numberOfPointsPattern = Pattern.compile("^\\s+number of point records:\\s+(\\d+)$");
    Pattern beginClassificationPattern = Pattern.compile("^histogram of classification of points:$");
    Pattern classificationPattern = Pattern.compile("^\\s+(\\d+)\\s+.+\\((\\d+)\\)\\s*$");
    boolean captureClassification = false;

    try {
      List<String> lines = Files.readAllLines(new File(pathToInfoFile).toPath());
      for (String line : lines) {
        applyPattern(line, boundingBoxMinPattern, Double::parseDouble, minX, minY, minZ);
        applyPattern(line, boundingBoxMaxPattern, Double::parseDouble, maxX, maxY, maxZ);
        applyPattern(line, crsPattern, (epsgCode) -> "EPSG:" + epsgCode, crs);
        applyPattern(line, warningPattern, (warningMessage) -> getWarningCounter() + 1, warningCounter);
        applyPattern(line, numberOfPointsPattern, Integer::parseInt, numberOfPoints);

        // The classifications are listed in a part like this one:
        // histogram of classification of points:
        //          361219  unclassified (1)
        //         5857112  ground (2)
        //              70  water (9)
        //          430321  Reserved for ASPRS Definition (20)
        // --> We need to find the header line and capture the class indices (number in brackets) afterwards.
        if (!captureClassification) {
          Matcher matcher = beginClassificationPattern.matcher(line);
          captureClassification = matcher.matches();
        } else {
          Matcher matcher = classificationPattern.matcher(line);
          if (matcher.matches()) {
            int classificationIndex = Integer.parseInt(matcher.group(2)); // group(1) is the amount of points in this group.
            classification.add(classificationIndex);
          }
        }
      }
    } catch (IOException e) {
      log.error("Could not read lasinfo output file at " + pathToInfoFile, e);
    }
  }

  /**
   * Applies the passed pattern on the passed line.
   * If it is successful, the passed converter converts each group value to the required type.
   * Each converted group value is stored in a passed values variable.
   * There have to be as many groups in the pattern as values variables are passed.
   * @param line The current line in the lasinfo output file.
   * @param pattern A regex pattern to be applied on the current line.
   * @param converter A converter function from String to the required group type.
   * @param values A list of values to store the converted group values.
   * @param <T> The type of the groups. All groups have to have the same type.
   */
  @SafeVarargs
  private final <T> void applyPattern(String line, Pattern pattern, Function<String, T> converter, MutableWrapper<T>... values) {
    Matcher matcher = pattern.matcher(line);
    if (matcher.matches()) {
      for (int i = 0; i < values.length; i++) {
        values[i].set(converter.apply(matcher.group(i + 1)));
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Lasinfo lasinfo = (Lasinfo)o;
    return Objects.equals(minX, lasinfo.minX) &&
            Objects.equals(minY, lasinfo.minY) &&
            Objects.equals(minZ, lasinfo.minZ) &&
            Objects.equals(maxX, lasinfo.maxX) &&
            Objects.equals(maxY, lasinfo.maxY) &&
            Objects.equals(maxZ, lasinfo.maxZ) &&
            Objects.equals(crs, lasinfo.crs) &&
            Objects.equals(warningCounter, lasinfo.warningCounter) &&
            Objects.equals(numberOfPoints, lasinfo.numberOfPoints) &&
            Objects.equals(classification, lasinfo.classification);
  }

}
