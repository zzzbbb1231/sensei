package com.senseidb.ba.realtime.domain;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;

public  class SingleValueSearchSnapshot implements ColumnSearchSnapshot<int[]> {
 

  private volatile int[] forwardIndex;
  private int forwardIndexSize;

  private ColumnType columnType;
  private AbstractDictionarySnapshot dictionarySnapshot;
 
  public void init(int[] forwardIndex, int forwardIndexSize, ColumnType columnType, AbstractDictionarySnapshot dictionarySnapshot) {   
    this.forwardIndex = forwardIndex;
    this.forwardIndexSize = forwardIndexSize;
    this.columnType = columnType;
    this.dictionarySnapshot = dictionarySnapshot;
    
  }
  

  public void setForwardIndexSize(int forwardIndexSize) {
    this.forwardIndexSize = forwardIndexSize;
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
   return null;
  }
  @Override
  public int getLength() {
    return getForwardIndexSize();
  }


  public AbstractDictionarySnapshot getDictionarySnapshot() {
    return dictionarySnapshot;
  }
  public boolean isSingleValue() {
    return forwardIndex instanceof int[];
  }
}
