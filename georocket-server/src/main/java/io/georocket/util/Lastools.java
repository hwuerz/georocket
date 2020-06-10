package io.georocket.util;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.exec.*;
import org.apache.commons.lang.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper around the command line application LAStools.
 * They are installed via gradle during the build.
 * This class provides methods to find the executables and run the tools on linux and windows.
 * @author Hendrik M. Wuerz
 */
public class Lastools {
  private static final Logger log = LoggerFactory.getLogger(Lastools.class);

  private final Vertx vertx;
  private final String lasmergeExecutable;
  private final String lasinfoExecutable;

  /**
   * Creates a new instance of the Lastools wrapper.
   * @param vertx The vertx object to get the GeoRocket home directory.
   *              The Lastools are installed relative to this dir, so it is required to find the executable.
   * @throws RuntimeException If the lastools executables could not be found.
   */
  public Lastools(Vertx vertx) {
    this.vertx = vertx;
    String home = vertx.getOrCreateContext().config().getString(ConfigConstants.HOME);
    lasmergeExecutable = getAbsolutePathToExecutable(home, Lastools::getLasmergeExecutable);
    lasinfoExecutable = getAbsolutePathToExecutable(home, Lastools::getLasinfoExecutable);
  }

  /**
   * Get the absolute path to the application returned by the <code>binarySelector</code>.
   * Searches relative to the passed <code>georocketHome</code> in different locations.
   * @param georocketHome The home directory of georocket as defined in the config.
   * @param binarySelector A function to return the application binary relative to LAStools home.
   *                       Used to select the correct version for the current OS.
   * @return The absolute path to the binary if <code>georocketHome</code> was absolute.
   */
  private static String getAbsolutePathToExecutable(String georocketHome, Function<String, String> binarySelector) {
    String[] possibleLasHomeDirs = new String[] {
            georocketHome + "/LAStools", // Run the built version.
            georocketHome + "/../georocket/georocket-server/LAStools" // Run from an extension in the IDE.
    };
    return Arrays.stream(possibleLasHomeDirs)
            .map(binarySelector)
            .filter(executable -> new File(executable).exists())
            .findAny()
            .orElseThrow(() -> new RuntimeException("LAStools are not installed. " +
                    binarySelector.apply("") + " not found. Tried " +
                    String.join(" ", possibleLasHomeDirs)) );
  }

  private static String getLasmergeExecutable(String lasHome) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return lasHome + "/bin/lasmerge.exe";
    } else {
      return lasHome + "/bin/lasmerge";
    }
  }

  private static String getLasinfoExecutable(String lasHome) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return lasHome + "/bin/lasinfo.exe";
    } else {
      return lasHome + "/bin/lasinfo";
    }
  }

  /**
   * Merges all files listed in <code>listOfFiles</code> into the <code>outputFile</code>.
   * Calls <code>handler</code> afterwards.
   * @param listOfFilesFile The path to a text file containing one path to a las / laz file in each line.
   * @param outputFile The output file where the merged point cloud should be written to.
   * @param handler A handler to be called after lastools finished.
   */
  public void lasmerge(String listOfFilesFile, String outputFile, Handler<AsyncResult<Void>> handler) {
    CommandLine cmdl = new CommandLine(lasmergeExecutable);
    cmdl.addArgument("-lof").addArgument(listOfFilesFile);
    cmdl.addArgument("-o").addArgument(outputFile);
    runCommandLine(cmdl, handler);
  }

  /**
   * Split the point cloud in <code>inputFile</code> into many small point clouds.
   * Store these small point clouds in <code>outputDir</code> with an increasing number as file name.
   * The format of the created files is the compressed laz.
   * @param inputFile The path of the input point cloud. This point cloud is split.
   * @param outputDir The output directory where to store the created split files.
   * @param chunkSize The max size of a created point cloud. (Only the last file might have less points.)
   * @param handler A handler to be called after lastools finished.
   */
  public void lasmerge(String inputFile, String outputDir, int chunkSize, Handler<AsyncResult<Void>> handler) {
    CommandLine cmdl = new CommandLine(lasmergeExecutable);
    cmdl.addArgument("-i").addArgument(inputFile);
    // Up to 9 billion chunks per file. If there are more, they are ignored.
    // In this case we would get problems with int in metadata too, so it is ok.
    cmdl.addArgument("-o").addArgument(outputDir + "/0000000000.laz");
    cmdl.addArgument("-split").addArgument(String.valueOf(chunkSize));
    runCommandLine(cmdl, handler);
  }

  /**
   * Get meta data for a las file.
   * @param lasFile The path to a las / laz file.
   * @param handler The handler to handle the extracted meta data.
   */
  public void lasinfo(String lasFile, Handler<AsyncResult<Lasinfo>> handler) {
    FileSystem fileSystem = vertx.fileSystem();
    String infoFile = fileSystem.createTempFileBlocking("lasinfo-result", ".txt");

    CommandLine cmdl = new CommandLine(lasinfoExecutable);
    cmdl.addArgument("-i").addArgument(lasFile);
    cmdl.addArgument("-o").addArgument(infoFile);
    cmdl.addArgument("-no_check_outside"); // Do not check the bounding box in the header to be correct.

    runCommandLine(cmdl, result -> {
      if (result.succeeded()) {
        Lasinfo lasinfo = new Lasinfo(infoFile);
        fileSystem.deleteBlocking(infoFile);
        handler.handle(Future.succeededFuture(lasinfo));
      } else {
        fileSystem.deleteBlocking(infoFile);
        handler.handle(Future.failedFuture(result.cause()));
      }
    });
  }

  /**
   * Executes the passed command line and calls the passed handler afterwards.
   * @param cmdl The command line to be executed.
   * @param handler The handler to handle the success / error response after execution.
   */
  private void runCommandLine(CommandLine cmdl, Handler<AsyncResult<Void>> handler) {
    Executor executor = new DefaultExecutor();
    executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
    executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));

    try {
      log.debug("Execute " + cmdl.toString());
      executor.execute(cmdl, new HashMap<>(System.getenv()), new DefaultExecuteResultHandler() {
        @Override
        public void onProcessComplete(final int exitValue) {
          log.info("LAStools quit with exit code: " + exitValue);
          handler.handle(Future.succeededFuture());
        }

        @Override
        public void onProcessFailed(final ExecuteException e) {
          log.error("LAStools execution failed", e);
          handler.handle(Future.failedFuture(e));
        }
      });
    } catch (IOException | RuntimeException e) {
      log.error("Propagate LAStools exception", e);
      handler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Java Object to store the las meta infos that are extracted from lasinfo.
   */
  static class Lasinfo {
    /**
     * Helper class to pass a primitive value by reference to other methods.
     * @param <T> The type of the stored value.
     */
    static class MutableWrapper<T> {
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
    }

    // All available meta infos.
    MutableWrapper<Double>
            minX = new MutableWrapper<>("minX"),
            minY = new MutableWrapper<>("minY"),
            minZ = new MutableWrapper<>("minZ"),
            maxX = new MutableWrapper<>("maxX"),
            maxY = new MutableWrapper<>("maxY"),
            maxZ = new MutableWrapper<>("maxZ");
    MutableWrapper<String> crs = new MutableWrapper<>("crs");
    HashSet<Integer> classification = new HashSet<>();

    /**
     * Copy the meta infos from a lasinfo output file to this object.
     * @param pathToInfoFile The path to the lasinfo output file.
     */
    public Lasinfo(String pathToInfoFile) {
      // Regex patterns. They are applied on each line. If they match, the infos are copied.
      Pattern boundingBoxXPattern = Pattern.compile("^\\s+X\\s+(\\d+)\\s+(\\d+)\\s*$");
      Pattern boundingBoxYPattern = Pattern.compile("^\\s+Y\\s+(\\d+)\\s+(\\d+)\\s*$");
      Pattern boundingBoxZPattern = Pattern.compile("^\\s+Z\\s+(\\d+)\\s+(\\d+)\\s*$");
      Pattern crsPattern = Pattern.compile("^\\s+.*ProjectedCSTypeGeoKey: (.+)$");
      Pattern beginClassificationPattern = Pattern.compile("^histogram of classification of points:$");
      Pattern classificationPattern = Pattern.compile("^\\s+(\\d+)\\s+.+\\((\\d+)\\)\\s*$");
      boolean captureClassification = false;

      try {
        List<String> lines = Files.readAllLines(new File(pathToInfoFile).toPath());
        for (String line : lines) {
          applyPattern(line, boundingBoxXPattern, Double::parseDouble, minX, maxX);
          applyPattern(line, boundingBoxYPattern, Double::parseDouble, minY, maxY);
          applyPattern(line, boundingBoxZPattern, Double::parseDouble, minZ, maxZ);
          applyPattern(line, crsPattern, Function.identity(), crs);

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
  }

}
