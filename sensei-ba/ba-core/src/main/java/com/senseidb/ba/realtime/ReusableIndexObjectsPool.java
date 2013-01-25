package com.senseidb.ba.realtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.realtime.domain.primitives.FieldRealtimeIndex;

public class ReusableIndexObjectsPool {
  private Schema schema;
  private int capacity;

  public void init(Schema schema, int capacity) {
    this.schema = schema;
    this.capacity = capacity;
  }
  private List<SegmentAppendableIndex> pool = new ArrayList<SegmentAppendableIndex>();
  private List<int[]> permutationPool = new ArrayList<int[]>();
  public synchronized SegmentAppendableIndex getAppendableIndex() {
    if (pool.size() > 0) {
      return pool.remove(pool.size() - 1);
    }
     SegmentAppendableIndex ret = new SegmentAppendableIndex();
     ret.init(schema, capacity);
     return ret;
  }
  public void recycle(SegmentAppendableIndex appendableIndex)  {
    appendableIndex.recycle();
    synchronized(this) {
      pool.add(appendableIndex);
    }
  }
  public void recycle(int[] permutationArray)  {
    synchronized(this) {
      //Arrays.fill(permutationArray, 0);
      permutationPool.add(permutationArray);
    }
  }
  public synchronized int[] getPermArray() {
    if (permutationPool.size() > 0) {
      return permutationPool.remove(permutationPool.size() - 1);
    }
    int[] ret = new int[capacity];
     return ret;
  }
}
