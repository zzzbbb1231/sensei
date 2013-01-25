package com.senseidb.ba.realtime.domain;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;

public  class SingleValueSearchSnapshot implements ColumnSearchSnapshot {
 

  private volatile int[] forwardIndex;
  private int forwardIndexSize;

  private ColumnType columnType;
  private DictionarySnapshot dictionarySnapshot;
 
  public void init(int[] forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot) {   
    this.forwardIndex = forwardIndex;
    this.forwardIndexSize = forwardIndexSize;
    this.columnType = columnType;
    this.dictionarySnapshot = dictionarySnapshot;
    
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


  public DictionarySnapshot getDictionarySnapshot() {
    return dictionarySnapshot;
  }
  
}
