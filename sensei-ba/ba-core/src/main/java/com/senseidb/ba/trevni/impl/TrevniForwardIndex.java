package com.senseidb.ba.trevni.impl;

import java.io.IOException;

import org.apache.trevni.ColumnValues;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ForwardIndex;

public class TrevniForwardIndex implements ForwardIndex {
  private ColumnValues<?> _colValReader;
  private int _length;
  private final TermValueList valueList;

  public TrevniForwardIndex(ColumnValues<?> colValreader, long count, TermValueList valueList) {
    _colValReader = colValreader;
    this.valueList = valueList;
    _length = (int) count;
  }

  @Override
  public int getLength() {
    return _length;
  }

  @Override
  public int getValueIndex(int docId) {
    try {
      _colValReader.seek((long) docId);
    } catch (IOException e) {
      e.printStackTrace();
      return 0;
    }

    Integer i = (Integer) _colValReader.next();
    return i.intValue();

  }

  @Override
  public int getFrequency(int valueId) {
    /*
     * Not going to be implemented in the first run
     */
    return 0;
  }

  @Override
  public TermValueList<?> getDictionary() {
    return valueList;
  }
}