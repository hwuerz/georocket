package io.georocket.index.las;

import io.georocket.index.generic.BoundingBoxIndexerFactory;
import io.georocket.index.xml.LasIndexer;
import io.georocket.index.xml.LasIndexerFactory;

/**
 * Create instances of {@link LasBoundingBoxIndexer}
 * @author Hendrik M. Wuerz
 */
public class LasBoundingBoxIndexerFactory extends BoundingBoxIndexerFactory
    implements LasIndexerFactory {
  @Override
  public LasIndexer createIndexer() {
    return new LasBoundingBoxIndexer();
  }
}
