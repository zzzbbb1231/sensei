package com.senseidb.ba.realtime.domain;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;

public  class SingleValueSearchSnapshot implements ColumnSearchSnapshot<int[]>, SingleValueForwardIndex {
 

  private volatile int[] forwardIndex;
  private volatile int forwardIndexSize;

  private ColumnType columnType;
  private DictionarySnapshot dictionarySnapshot;
 
  public void init(int[] forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot) {   
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
   return (TermValueList<?>) dictionarySnapshot;
  }
  @Override
  public int getLength() {
    return getForwardIndexSize();
  }


  public DictionarySnapshot getDictionarySnapshot() {
    return dictionarySnapshot;
  }
  public boolean isSingleValue() {
    return forwardIndex instanceof int[];
  }


  @Override
  public SingleValueRandomReader getReader() {
    
    return new SingleValueRandomReader() {
      
      @Override
      public int getValueIndex(int docId) {
        return forwardIndex[docId];
      }
    };
  }
}
