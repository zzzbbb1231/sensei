package com.senseidb.ba.realtime.domain;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;
import com.senseidb.ba.realtime.domain.multi.MultiArray;

public  class MultiValueSearchSnapshot implements ColumnSearchSnapshot<MultiArray>, MultiValueForwardIndex {
 

  private volatile MultiArray forwardIndex;
  private int forwardIndexSize;

  private ColumnType columnType;
  private DictionarySnapshot dictionarySnapshot;
 
  public void init(MultiArray forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot) {   
    this.forwardIndex = forwardIndex;
    this.forwardIndexSize = forwardIndexSize;
    this.columnType = columnType;
    this.dictionarySnapshot = dictionarySnapshot;
    
  }
  

  public void setForwardIndexSize(int forwardIndexSize) {
    this.forwardIndexSize = forwardIndexSize;
  }


  public MultiArray getForwardIndex() {
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
    return false;
  }


  @Override
  public MultiFacetIterator getIterator() {
    return forwardIndex.iterator(dictionarySnapshot.getInvPermutationArray());
  }


  @Override
  public int randomRead(int[] buffer, int index) {
    return forwardIndex.readValues(buffer, index);
  }


  @Override
  public int getMaxNumValuesPerDoc() {
    return forwardIndex.getMaxNumValuesPerDoc();
  }
}
