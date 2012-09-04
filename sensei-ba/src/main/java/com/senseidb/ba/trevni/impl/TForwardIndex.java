package com.senseidb.ba.trevni.impl;

import java.io.IOException;

import org.apache.trevni.ColumnValues;

import com.senseidb.ba.trevni.*;

public class TForwardIndex implements ForwardIndex {
  ColumnValues<?> _colValReader;
  String _colType;

  public TForwardIndex(ColumnValues<?> colValreader, String type) {
    _colValReader = colValreader;
    _colType = type;
  }

  @Override
  public int getLength() {
    return 0;
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

    return 0;
  }
}
