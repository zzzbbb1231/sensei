package com.senseidb.ba.trevni.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import com.senseidb.ba.trevni.*;

import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;


public class TrevniDictionary<T> implements TermValueList {
  
  private HashMap<Integer, TrevniTermValue<T>> _dictionaryMap;
  private String _keyType;
  private String _valType;

  public TrevniDictionary(File file, Class<?> originalValType) throws IOException {
    _dictionaryMap = new HashMap<Integer, TrevniTermValue<T>>();
    ColumnFileReader reader = new ColumnFileReader(file);
    ColumnValues<String> val = reader.getValues(0);
    while (val.hasNext()) {
      String[] pair = val.next().split(":");
      String original = pair[0];
      Integer mapped = Integer.parseInt(pair[1]);
      _dictionaryMap.put(mapped, applyProperCastAndReturnValue(original, originalValType));
    }
  }

  @SuppressWarnings("unchecked")
  public TrevniTermValue<T> applyProperCastAndReturnValue(String original, Class<?> originalValType) {
    return new TrevniTermValue<T>((T) original);
  }

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public Comparable<?> getTermValue(int index) {
    Integer i = new Integer(index);
    return _dictionaryMap.get(i);
  }

}
