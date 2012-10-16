package com.senseidb.ba.gazelle.utils.multi;

public interface MultiFacetIterator {

  public abstract boolean advance(int index);

  public abstract int readValues(int[] buffer);

  int find(int fromIndex, int value);

}