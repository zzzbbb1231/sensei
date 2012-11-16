package com.senseidb.ba.format;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class AbstractDataSource implements GazelleDataSource {
  private Set<AbstractGazelleIterator> iterators = new HashSet<AbstractGazelleIterator>();

  @Override
  public Iterator<Map<String, Object>> newIterator() {
    AbstractGazelleIterator ret = createIterator();
    iterators.add(ret);
    return ret;
  }

  public abstract AbstractGazelleIterator createIterator();

  @Override
  public void closeCurrentIterators() {
    for (AbstractGazelleIterator iterator : iterators) {
      iterator.close();
    }

  }
}