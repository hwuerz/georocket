package io.georocket.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Function;

/**
 * Wrapper around the command line application LAStools.
 * They are installed via gradle during the build.
 * This class provides methods to find the executables and run the tools on linux and windows.
 * @author Hendrik M. Wuerz
 */
public class Lastools {
  private static final Logger log = LoggerFactory.getLogger(Lastools.class);

  private final Vertx vertx;

  /**
   * The absolute, system dependent path to the lasmerge executable.
   */
  private final String lasmergeExecutable;

  /**
   * The absolute, system dependent path to the lasinfo executable.
   */
  private final String lasinfoExecutable;

  /**
   * Creates a new instance of the Lastools wrapper.
   * @param vertx The vertx object to get the GeoRocket home directory.
   *              The Lastools are installed relative to this dir, so it is required to find the executable.
   * @param home  The GeoRocket home directory. The LAStools installation is searched relative to this location.
   * @throws RuntimeException If the lastools executables could not be found.
   */
  public Lastools(Vertx vertx, String home) {
    this.vertx = vertx;
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
            georocketHome + "/../georocket/georocket-common/LAStools", // Run from an extension in the IDE.
            georocketHome + "/../georocket-common/LAStools" // Run from another package.
    };
    if (georocketHome == null) {
      log.error("Lastools is called with null as georocket home ", new RuntimeException());
    }
    return Arrays.stream(possibleLasHomeDirs)
            .map(binarySelector)
            .filter(executable -> new File(executable).exists())
            .findAny()
            .orElseThrow(() -> new RuntimeException("LAStools are not installed. " +
                    binarySelector.apply("") + " not found. Tried " +
                    String.join(" ", possibleLasHomeDirs)));
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

}
