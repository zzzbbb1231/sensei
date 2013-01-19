package com.senseidb.ba.realtime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.senseidb.ba.realtime.primitives.FieldRealtimeIndex;

public class AppendableIndexPool {
  private Schema schema;
  private int capacity;

  public void init(Schema schema, int capacity) {
    this.schema = schema;
    this.capacity = capacity;
  }
  private List<SegmentAppendableIndex> pool = new ArrayList<SegmentAppendableIndex>();
  public synchronized SegmentAppendableIndex getAppendableIndex() {
    if (pool.size() > 0) {
      return pool.remove(pool.size() - 1);
    }
     SegmentAppendableIndex ret = new SegmentAppendableIndex();
     ret.init(schema, capacity);
     return ret;
  }
  public void recycle(SegmentAppendableIndex appendableIndex)  {
    for (FieldRealtimeIndex index :   appendableIndex.getColumnIndexes()) {
      index.recycle();
    }
    synchronized(this) {
      pool.add(appendableIndex);
    }
  }
  
}
