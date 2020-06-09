package io.georocket.storage;

import io.vertx.core.json.JsonObject;

/**
 * Metadata for a LAS chunk
 * @author Hendrik Wuerz
 */
public class LasChunkMeta extends ChunkMeta {
  /**
   * The mime type for LAS chunks
   */
  public static final String MIME_TYPE = "application/vnd.las";

  /**
   * Create a new metadata object
   * @param number The number of this chunk in the total blob.
   */
  public LasChunkMeta(int number) {
    super(number, number + 1, MIME_TYPE);
  }

  /**
   * Create a new metadata object from a JsonObject
   * @param json the JsonObject
   */
  public LasChunkMeta(JsonObject json) {
    super(json);
  }
}
