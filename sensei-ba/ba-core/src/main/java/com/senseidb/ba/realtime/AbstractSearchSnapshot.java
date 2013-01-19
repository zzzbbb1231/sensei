package com.senseidb.ba.realtime;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;

public abstract class AbstractSearchSnapshot implements ColumnSearchSnapshot {
  protected IntList permutationArray;

  private volatile int[] forwardIndex;
  private int forwardIndexSize;

  private ColumnType columnType;
 
  public void initForwardIndex(int[] forwardIndex, int forwardIndexSize, ColumnType columnType) {   
    this.forwardIndex = forwardIndex;
    this.forwardIndexSize = forwardIndexSize;
    this.columnType = columnType;
    
  }
  @Override
  public int size() {
    return permutationArray.size();
  }
  @Override
  public IntList getPermutationArray() {
    return permutationArray;
  }

  public int[] getForwardIndex() {
    return forwardIndex;
  }

  public int getForwardIndexSize() {
    return forwardIndexSize;
  }
  @Override
  public ColumnType getColumnType() {
    return columnType;
  }
  @Override
  public TermValueList<?> getDictionary() {
    throw new UnsupportedOperationException();
  }
  @Override
  public int getLength() {
    return getForwardIndexSize();
  }
}
