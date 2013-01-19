package com.senseidb.ba.realtime.primitives;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.realtime.ColumnSearchSnapshot;

public abstract class AbstractFieldRealtimeIndex implements FieldRealtimeIndex {
  protected final int capacity;
  protected int[] forwardIndex;
  protected int currentPosition;
  protected ColumnSearchSnapshot searchSnapshot;
  public AbstractFieldRealtimeIndex(int capacity) {
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
    searchSnapshot.recycle();
    
  }
}
