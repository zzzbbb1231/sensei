package com.senseidb.ba.realtime.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.realtime.ColumnSearchSnapshot;

public interface FieldRealtimeIndex {
  public static final int NULL_DICTIONARY_ID = 1;
  public int getCurrentSize();
  public int getCapacity();
  public void addElement(Object value, ReadWriteLock readWriteLock);
  public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock);
  public void recycle();
  
}