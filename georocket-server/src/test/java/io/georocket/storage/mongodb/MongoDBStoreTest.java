package io.georocket.storage.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Before;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import io.georocket.constants.ConfigConstants;
import io.georocket.storage.StorageTest;
import io.georocket.storage.Store;
import io.georocket.util.PathUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.embeddedmongo.EmbeddedMongoVerticle;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;

/**
 * Test {@link MongoDBStore}
 * @author Andrej Sajenko
 */
public class MongoDBStoreTest extends StorageTest {
  private static long MAX_WORKER_EXECUTION_TIME = 30 * 60 * 1000;

  /**
   * Default constructor
   */
  public MongoDBStoreTest() {
    super.rule = new RunTestOnContext(new VertxOptions()
        .setMaxWorkerExecuteTime(MAX_WORKER_EXECUTION_TIME));
  }

  private static String MONGODB_DBNAME = "testdb";
  private InetSocketAddress serverAddress = new InetSocketAddress("localhost", 5005);

  /**
   * Set up test dependencies.
   */
  @Before
  public void setUp() {
    Vertx vertx = rule.vertx();

    vertx.getOrCreateContext().config().put("port", serverAddress.getPort());

    vertx.deployVerticle(
        EmbeddedMongoVerticle.class.getName(),
        new DeploymentOptions()
            .setWorker(true)
            .setConfig(vertx.getOrCreateContext().config())
    );
  }

  private void configureVertx(Vertx vertx) {
    JsonObject config = vertx.getOrCreateContext().config();

    config.put(ConfigConstants.STORAGE_MONGODB_HOST, serverAddress.getHostName());
    config.put(ConfigConstants.STORAGE_MONGODB_PORT, serverAddress.getPort());
    config.put(ConfigConstants.STORAGE_MONGODB_DATABASE, MONGODB_DBNAME);
  }

  @Override
  protected Store createStore(Vertx vertx) {
    configureVertx(vertx);
    return new MongoDBStore(vertx);
  }

  @Override
  protected void prepareData(TestContext context, Vertx vertx, String path,
      Handler<AsyncResult<String>> handler) {
    String filename = PathUtils.join(path, ID);
    vertx.<String>executeBlocking(f -> {
      try (MongoClient client = new MongoClient(new ServerAddress(serverAddress))) {
        DB db = client.getDB(MONGODB_DBNAME);
        GridFS gridFS = new GridFS(db);
        GridFSInputFile file = gridFS.createFile(filename);
        try (
          OutputStream os = file.getOutputStream();
          OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
        ) {
          writer.write(CHUNK_CONTENT);
          f.complete(filename);
        }
      } catch (IOException ex) {
        f.fail(ex);
      }
    }, handler);
  }

  @Override
  protected void validateAfterStoreAdd(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);
      context.assertFalse(files.isEmpty());

      GridFSDBFile file = files.get(0);
      InputStream is = file.getInputStream();

      String content = null;
      try {
        content = IOUtils.toString(is, Charsets.UTF_8);
      } catch (IOException ex) {
        context.fail("Could not read GridDSDBFile: " + ex.getMessage());
      }

      context.assertEquals(CHUNK_CONTENT, content);

      client.close();

      f.complete();
    }, handler);
  }

  @Override
  protected void validateAfterStoreDelete(TestContext context, Vertx vertx,
      String path, Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(f -> {
      MongoClient client = new MongoClient(new ServerAddress(serverAddress));
      DB db = client.getDB(MONGODB_DBNAME);
      GridFS gridFS = new GridFS(db);

      DBObject query = new BasicDBObject();

      List<GridFSDBFile> files = gridFS.find(query);
      context.assertTrue(files.isEmpty());

      client.close();

      f.complete();
    }, handler);
  }
}