package com.senseidb.ba.realtime.domain.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.multi.MultiArray;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.RealtimeDictionary;

public  class SingleFieldRealtimeIndex implements FieldRealtimeIndex {
  protected final int capacity;
  protected int[] forwardIndex;
  protected int currentPosition;
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
  public void recycle() {
    Arrays.fill(forwardIndex, 0);
    currentPosition = 0;
    searchSnapshot.getDictionarySnapshot().recycle();
  }
  @Override
  public void addElement(Object value, ReadWriteLock readWriteLock) {
    int dictionaryId = realtimeDictionary.addElement(value, readWriteLock);
    forwardIndex[currentPosition] = dictionaryId;
    currentPosition++;
  }
  @Override
  public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock) {
    if (searchSnapshot != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray() != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray().size() == realtimeDictionary.size()) {
      searchSnapshot.setForwardIndexSize(currentPosition);
    } else {
      SingleValueSearchSnapshot singleValueSearchSnapshot = new SingleValueSearchSnapshot();
     singleValueSearchSnapshot.init(forwardIndex, currentPosition, columnType, (AbstractDictionarySnapshot)realtimeDictionary.produceDictSnapshot(readWriteLock));
     searchSnapshot = singleValueSearchSnapshot;
    }
    return searchSnapshot;
  }
}
