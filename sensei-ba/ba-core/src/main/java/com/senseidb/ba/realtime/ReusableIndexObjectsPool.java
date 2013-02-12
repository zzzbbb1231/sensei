package com.senseidb.ba.realtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.util.Assert;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
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
  private Map<String, List<DictionarySnapshot>> dictionaryCache = new HashMap<String, List<DictionarySnapshot>>();  
  
  public synchronized SegmentAppendableIndex getAppendableIndex() {
    
    SegmentAppendableIndex ret = null;
    if (pool.size() > 0) {
      ret =  pool.remove(pool.size() - 1);
    } else {
     ret = new SegmentAppendableIndex(this);
     
     ret.init(schema, capacity);
    }
    ret.getSegmentResurrectingMarker().incRef();
     return ret;
  }
  
  public synchronized void recycle(DictionarySnapshot dictionarySnapshot, String column)  {
     
    dictionarySnapshot.getResurrectingMarker().reset();
    if (!dictionaryCache.containsKey(column)) {
      dictionaryCache.put(column, new ArrayList<DictionarySnapshot>());
    }
  
    dictionarySnapshot.recycle();
    Assert.state(!dictionaryCache.get(column).contains(dictionarySnapshot));
    dictionaryCache.get(column).add(dictionarySnapshot);
  }
  public synchronized DictionarySnapshot getDictSnapshot(String column)  {
    List<DictionarySnapshot> list = dictionaryCache.get(column);
    DictionarySnapshot ret = null;
    if (list == null || list.size() == 0) {
      return null;
    }
    
    ret = list.remove(list.size() - 1);
    Assert.state(ret.getResurrectingMarker().getValue() == 0, "" + ret.getResurrectingMarker().getValue());
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
