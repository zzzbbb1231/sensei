package com.senseidb.ba.realtime.domain.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.multi.MultiArray;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.RealtimeDictionary;

public  class SingleFieldRealtimeIndex implements FieldRealtimeIndex {
  protected final int capacity;
  protected int[] forwardIndex;
  protected volatile int currentPosition;
  protected ColumnSearchSnapshot searchSnapshot;
  private final RealtimeDictionary realtimeDictionary;
  private final ColumnType columnType;
  public SingleFieldRealtimeIndex(RealtimeDictionary realtimeDictionary, ColumnType columnType, int capacity) {
    this.realtimeDictionary = realtimeDictionary;
    this.columnType = columnType;
    this.capacity = capacity;
    currentPosition = 0;
    forwardIndex = new int[capacity];
  }
  @Override
  public int getCurrentSize() {
    return currentPosition;
  }

  @Override
  public int getCapacity() {
    return capacity;
  }
 
  @Override
  public void addElement(Object value, ReadWriteLock readWriteLock) {
    int dictionaryId = realtimeDictionary.addElement(value, readWriteLock);
    forwardIndex[currentPosition] = dictionaryId;
    currentPosition++;
  }
  @Override
  public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock, ReusableIndexObjectsPool reusableIndexObjectsPool, String columnName) {
    if (searchSnapshot != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray() != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray().size() == realtimeDictionary.size()) {
      searchSnapshot.setForwardIndexSize(currentPosition);
    } else {
      if (searchSnapshot != null) {
        searchSnapshot.getDictionarySnapshot().getResurrectingMarker().decRef();
      }
      int position = currentPosition;
      SingleValueSearchSnapshot singleValueSearchSnapshot = new SingleValueSearchSnapshot();
     DictionarySnapshot dictSnapshot = (DictionarySnapshot)realtimeDictionary.produceDictSnapshot(readWriteLock, reusableIndexObjectsPool, columnName);
    singleValueSearchSnapshot.init(forwardIndex, position, columnType, dictSnapshot);
    dictSnapshot.getResurrectingMarker().incRef();
     searchSnapshot = singleValueSearchSnapshot;
    }
    return searchSnapshot;
  }
  
  @Override
  public void recycle() {
    Arrays.fill(forwardIndex, 0);
    currentPosition = 0;
    searchSnapshot.getDictionarySnapshot().recycle();
    realtimeDictionary.recycle();
  }
}
