package io.georocket.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Test {@link Lastools}
 * @author Hendrik M. Wuerz
 */
@RunWith(VertxUnitRunner.class)
public class LastoolsTest {

    /**
     * A tmp directory. Used to generate split results.
     */
    private String tmpDirectory;

    /**
     * A tmp *.txt file. Used to write a list of files.
     */
    private String tmpTxtFile;

    /**
     * A tmp *.laz file. Used as an output for merging.
     */
    private String tmpLazFile;

    /**
     * Run the test on a Vert.x test context
     */
    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp() {
        FileSystem fileSystem = rule.vertx().fileSystem();
        tmpDirectory = fileSystem.createTempDirectoryBlocking("tmpDirectory");
        tmpTxtFile = fileSystem.createTempFileBlocking("tmpTxtFile", ".txt");
        tmpLazFile = fileSystem.createTempFileBlocking("tmpLazFile", ".laz");
    }

    @After
    public void cleanUp() {
        FileSystem fileSystem = rule.vertx().fileSystem();
        fileSystem.deleteRecursiveBlocking(tmpDirectory, true);
        fileSystem.deleteBlocking(tmpTxtFile);
        fileSystem.deleteBlocking(tmpLazFile);
    }

    /**
     * Get the lasinfo response and check it to be correct.
     */
    @Test
    public void lasinfoTest(TestContext context) {
        Async async = context.async();
        Lastools lastools = new Lastools(rule.vertx(), "./");

        String lazFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.laz")
                .getPath();
        String lasinfoFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.lasinfo.txt")
                .getPath();
        lastools.lasinfo(lazFile, asyncResult -> {
            if (asyncResult.failed()) {
                context.fail(asyncResult.cause());
                async.complete();
                return;
            }
            Lasinfo result = asyncResult.result();
            Lasinfo expected = new Lasinfo(lasinfoFile);
            context.assertEquals(expected, result);
            async.complete();
        });
    }

    /**
     * Split a laz file. Ensure no points to be lost.
     */
    @Test
    public void lasmergeSplitTest(TestContext context) {
        Async async = context.async();
        Lastools lastools = new Lastools(rule.vertx(), "./");

        String lazFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.laz")
                .getPath();

        FileSystem fileSystem = rule.vertx().fileSystem();

        lastools.lasmerge(lazFile, tmpDirectory, 1000, handler -> {
            context.assertTrue(handler.succeeded());
            List<String> splitFiles = fileSystem.readDirBlocking(tmpDirectory);
            context.assertEquals(2, splitFiles.size());

            // Get lasinfo for both files. (The original file has 1001 points, we split every 1000 points --> 2 files)
            lastools.lasinfo(splitFiles.get(0), lasinfo1 -> {
                if (lasinfo1.failed()) context.fail(lasinfo1.cause());
                lastools.lasinfo(splitFiles.get(1), lasinfo2 -> {
                    if (lasinfo2.failed()) context.fail(lasinfo2.cause());

                    // One file has to have 1000 points, the other one only one.
                    context.assertTrue(lasinfo1.result().getNumberOfPoints() == 1000 && lasinfo2.result().getNumberOfPoints() == 1 ||
                            lasinfo1.result().getNumberOfPoints() == 1 && lasinfo2.result().getNumberOfPoints() == 1000);

                    async.complete();
                });
            });
        });
    }

    /**
     * Merge laz files together and ensure a correct result.
     */
    @Test
    public void lasmergeMergeTest(TestContext context) {
        Async async = context.async();
        Lastools lastools = new Lastools(rule.vertx(), "./");

        String lazFilesDir = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points-split500/")
                .getPath();
        String lasinfoFile = Lastools.class.getClassLoader()
                .getResource("io/georocket/util/pointCloud-1001-points.lasinfo.txt")
                .getPath();

        FileSystem fileSystem = rule.vertx().fileSystem();
        List<String> inputFiles = fileSystem.readDirBlocking(lazFilesDir);

        String listOfFiles = tmpTxtFile;
        Buffer listOfFilesBuffer = Buffer.buffer()
                .appendString(String.join("\n", inputFiles));
        fileSystem.writeFileBlocking(listOfFiles, listOfFilesBuffer);

        lastools.lasmerge(listOfFiles, tmpLazFile, v -> {
            lastools.lasinfo(tmpLazFile, lasinfo -> {
                context.assertTrue(lasinfo.succeeded());
                Lasinfo expected = new Lasinfo(lasinfoFile);
                context.assertEquals(expected, lasinfo.result());
                async.complete();
            });
        });
    }

}
