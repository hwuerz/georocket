package io.georocket.index.xml;

import io.georocket.index.IndexerFactory;

/**
 * Factory for {@link LasIndexer} objects
 * @author Hendrik M. Wuerz
 */
public interface LasIndexerFactory extends IndexerFactory {
  /**
   * @return a new instance of {@link LasIndexer}
   */
  @Override
  LasIndexer createIndexer();
}
