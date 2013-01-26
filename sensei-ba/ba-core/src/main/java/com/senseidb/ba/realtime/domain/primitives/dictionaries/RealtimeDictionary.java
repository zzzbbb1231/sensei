package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

public interface RealtimeDictionary {
  public static final int NULL_DICTIONARY_ID = 1;
  public void init();
  public DictionarySnapshot produceDictSnapshot(ReadWriteLock readWriteLock);
  public int addElement(Object element, ReadWriteLock readWriteLock);
  public int size();
}
