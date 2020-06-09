package io.georocket;

import io.georocket.constants.ConfigConstants;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.exec.*;
import org.apache.commons.lang.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Wrapper around the command line application LAStools.
 * They are installed via gradle during the build.
 * This class provides methods to find the executables and run the tools on linux and windows.
 */
public class Lastools {
  private static final Logger log = LoggerFactory.getLogger(Lastools.class);

  private final String lasmergeExecutable;

  /**
   * Creates a new instance of the Lastools wrapper.
   * @param vertx The vertx object to get the GeoRocket home directory.
   *              The Lastools are installed relative to this dir, so it is required to find the executable.
   * @throws RuntimeException If the lastools executables could not be found.
   */
  public Lastools(Vertx vertx) {
    String home = vertx.getOrCreateContext().config().getString(ConfigConstants.HOME);
    String[] possibleLasHomeDirs = new String[] {
            home + "/LAStools", // Run the built version.
            home + "/../georocket/georocket-server/LAStools" // Run from an extension in the IDE.
    };
    lasmergeExecutable = Arrays.stream(possibleLasHomeDirs)
            .map(Lastools::getLasMergeExecutable)
            .filter(executable -> new File(executable).exists())
            .findAny()
            .orElseThrow(() -> new RuntimeException("LAStools are not installed. lasmerge not found. Tried " +
                    String.join(" ", possibleLasHomeDirs)) );
  }

  private static String getLasMergeExecutable(String lasHome) {
    if (SystemUtils.IS_OS_WINDOWS) {
      return lasHome + "/bin/lasmerge.exe";
    } else {
      return lasHome + "/bin/lasmerge";
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

  private void runCommandLine(CommandLine cmdl, Handler<AsyncResult<Void>> handler) {
    Executor executor = new DefaultExecutor();
    executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
    executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));

    try {
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

}
