package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import java.util.concurrent.atomic.AtomicInteger;

import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

public class DictionaryResurrectingMarker {
  private String columnName;
  private ReusableIndexObjectsPool indexObjectsPool;
  private DictionarySnapshot dictionarySnapshot;
  private AtomicInteger counter = new AtomicInteger(0);
  public DictionaryResurrectingMarker(String columnName, ReusableIndexObjectsPool indexObjectsPool, DictionarySnapshot dictionarySnapshot) {
    super();
    this.columnName = columnName;
    this.indexObjectsPool = indexObjectsPool;
    this.dictionarySnapshot = dictionarySnapshot;
  }
  public void incRef() {
    counter.incrementAndGet();
  }
  public void decRef() {
    int res = counter.decrementAndGet();   
    if (res <= 0) {
      counter.set(0);
      indexObjectsPool.recycle(dictionarySnapshot, columnName);
    }
  }
  public void reset() {
    counter.set(0);
  }
  public int getValue() {
      return counter.get();
  }
}
