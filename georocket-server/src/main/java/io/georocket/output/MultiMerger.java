package io.georocket.output;

import io.georocket.output.geojson.GeoJsonMerger;
import io.georocket.output.las.LasMerger;
import io.georocket.output.xml.XMLMerger;
import io.georocket.storage.ChunkMeta;
import io.georocket.storage.ChunkReadStream;
import io.georocket.storage.GeoJsonChunkMeta;
import io.georocket.storage.JsonChunkMeta;
import io.georocket.storage.LasChunkMeta;
import io.georocket.storage.XMLChunkMeta;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import rx.Completable;

/**
 * <p>A merger that either delegates to {@link XMLMerger}, {@link GeoJsonMerger}
 * or {@link LasMerger} depending on the types of the chunks to merge.</p>
 * <p>For the time being the merger can only merge chunks of the same type.
 * In the future it may create an archive (e.g. a ZIP or a TAR file) containing
 * chunks of mixed types.</p>
 * @author Michel Kraemer
 */
public class MultiMerger implements Merger<ChunkMeta> {
  private XMLMerger xmlMerger;
  private GeoJsonMerger geoJsonMerger;
  private LasMerger lasMerger;

  /**
   * {@code true} if chunks should be merged optimistically without
   * prior initialization
   */
  private final boolean optimistic;

  private final Vertx vertx;

  /**
   * Creates a new merger
   * @param optimistic {@code true} if chunks should be merged optimistically
   * without prior initialization
   */
  public MultiMerger(boolean optimistic, Vertx vertx) {
    this.optimistic = optimistic;
    this.vertx = vertx;
  }

  private Completable getError(String given, String existing) {
    return Completable.error(new IllegalStateException(
            "Cannot merge " + given + " chunk into a " + existing + " document."));
  }

  private Completable ensureMerger(ChunkMeta meta) {
    if (meta instanceof XMLChunkMeta) {
      if (xmlMerger == null) {
        if (geoJsonMerger != null) return getError("XML", "GeoJSON");
        if (lasMerger != null) return getError("XML", "LAS");
        xmlMerger = new XMLMerger(optimistic);
      }
      return Completable.complete();
    } else if (meta instanceof GeoJsonChunkMeta) {
      if (geoJsonMerger == null) {
        if (xmlMerger != null) return getError("GeoJSON", "XML");
        if (lasMerger != null) return getError("GeoJSON", "LAS");
        geoJsonMerger = new GeoJsonMerger(optimistic);
      }
      return Completable.complete();
    } else if (meta instanceof LasChunkMeta) {
      if (lasMerger == null) {
        if (xmlMerger != null) return getError("LAS", "XML");
        if (geoJsonMerger != null) return getError("LAS", "GeoJSON");
        lasMerger = new LasMerger(vertx);
      }
      return Completable.complete();
    }
    return Completable.error(new IllegalStateException("Cannot merge "
      + "chunk of type " + meta.getMimeType()));
  }
  
  @Override
  public Completable init(ChunkMeta meta) {
    return ensureMerger(meta)
      .andThen(Completable.defer(() -> {
        if (meta instanceof XMLChunkMeta) {
          return xmlMerger.init((XMLChunkMeta)meta);
        } else if (meta instanceof GeoJsonChunkMeta) {
          return geoJsonMerger.init((GeoJsonChunkMeta)meta);
        }
        return lasMerger.init((LasChunkMeta)meta);
      }));
  }

  @Override
  public Completable merge(ChunkReadStream chunk, ChunkMeta meta,
      WriteStream<Buffer> out) {
    return ensureMerger(meta)
      .andThen(Completable.defer(() -> {
        if (meta instanceof XMLChunkMeta) {
          return xmlMerger.merge(chunk, (XMLChunkMeta)meta, out);
        } else if (meta instanceof JsonChunkMeta) {
          return geoJsonMerger.merge(chunk, (GeoJsonChunkMeta)meta, out);
        }
        return lasMerger.merge(chunk, (LasChunkMeta)meta, out);
      }));
  }

  @Override
  public Completable finish(WriteStream<Buffer> out) {
    if (xmlMerger != null) {
      return xmlMerger.finish(out);
    }
    if (geoJsonMerger != null) {
      return geoJsonMerger.finish(out);
    }
    if (lasMerger != null) {
      return lasMerger.finish(out);
    }
    return Completable.error(new RuntimeException("No merger found."));
  }
}
