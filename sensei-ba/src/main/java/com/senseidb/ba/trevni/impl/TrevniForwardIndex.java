package com.senseidb.ba.trevni.impl;

import java.io.IOException;

import org.apache.trevni.ColumnValues;

import com.senseidb.ba.trevni.*;

public class TrevniForwardIndex implements ForwardIndex {
  private ColumnValues<?> _colValReader;
  private String _colType;
  private int _length;

  public TrevniForwardIndex(ColumnValues<?> colValreader, String type, long count) {
    _colValReader = colValreader;
    _colType = type;
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
    if (_colType.equals("dim") || _colType.equals("shrd") || _colType.equals("sort")) {
      Integer i = (Integer) _colValReader.next();
      return i.intValue();
    } else if (_colType.equals("time")) {
      Long f = (Long) _colValReader.next();
      return f.intValue();
    } else {
      Double d = (Double) _colValReader.next();
      return d.intValue();
    }
  }

  @Override
  public int getFrequency(int valueId) {
    /*
     * Not going to be implemented in the first run
     * */
    return 0;
  }
}
