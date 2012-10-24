package com.senseidb.ba.gazelle.utils.multi;

import com.browseengine.bobo.util.BigSegmentedArray;

public interface MultiFacetIterator {

  public abstract boolean advance(int index);

  public abstract int readValues(int[] buffer);

  int find(int fromIndex, int value);
  void count(BigSegmentedArray counts);
}