package com.senseidb.ba.realtime.domain.primitives;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.concurrent.locks.ReadWriteLock;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.MultiValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.multi.MultiArray;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.RealtimeDictionary;

public class MultiFieldRealtimeIndex implements FieldRealtimeIndex {
  protected final int capacity;
  protected MultiArray forwardIndex;
  protected volatile int currentPosition;
  protected ColumnSearchSnapshot searchSnapshot;
  private final RealtimeDictionary realtimeDictionary;
  private final ColumnType columnType;
  private IntList buffer = new IntArrayList();
  public MultiFieldRealtimeIndex(RealtimeDictionary realtimeDictionary, ColumnType columnType, int capacity) {
    this.realtimeDictionary = realtimeDictionary;
    this.columnType = columnType;
    this.capacity = capacity;
    currentPosition = 0;
    forwardIndex = new MultiArray(capacity);
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
    forwardIndex.recycle();
    currentPosition = 0;
    searchSnapshot.getDictionarySnapshot().recycle();
    realtimeDictionary.recycle();
  }
  @Override
  public void addElement(Object value, ReadWriteLock readWriteLock) {
    buffer.clear();
    if (value == null) {
      int dictionaryId = realtimeDictionary.addElement(value, readWriteLock);
      buffer.add(dictionaryId);
      forwardIndex.addNumbers(buffer);
    } else {
      if (value instanceof Object[])
      for (Object element : (Object[]) value) {
        int dictionaryId = realtimeDictionary.addElement(element, readWriteLock);
        buffer.add(dictionaryId);
      } else {
        int dictionaryId = realtimeDictionary.addElement(value, readWriteLock);
        buffer.add(dictionaryId);
      }
      forwardIndex.addNumbers(buffer);
    }    
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
      MultiValueSearchSnapshot multiValueSearchSnapshot = new MultiValueSearchSnapshot();
      int position = currentPosition;
      DictionarySnapshot dictSnapshot = (DictionarySnapshot)realtimeDictionary.produceDictSnapshot(readWriteLock, reusableIndexObjectsPool,columnName);
      dictSnapshot.getResurrectingMarker().incRef();
      multiValueSearchSnapshot.init(forwardIndex, position, columnType, dictSnapshot);
     searchSnapshot = multiValueSearchSnapshot;
    }
    return searchSnapshot;
  }
}
