package com.senseidb.ba.realtime;

import java.util.concurrent.atomic.AtomicInteger;

public class SegmentResurrectingMarker {
    private ReusableIndexObjectsPool indexObjectsPool;
    private SegmentAppendableIndex appendableIndex;
    private AtomicInteger counter = new AtomicInteger(0);
    public SegmentResurrectingMarker(ReusableIndexObjectsPool indexObjectsPool, SegmentAppendableIndex appendableIndex) {
      super();
      this.indexObjectsPool = indexObjectsPool;
      this.appendableIndex = appendableIndex;
    };
   /* @Override
    protected void finalize() throws Throwable {
      indexObjectsPool.recycle(appendableIndex);
    }*/
    public void incRef() {
      counter.incrementAndGet();
    }
    public void decRef() {
      int res = counter.decrementAndGet();
      if (res <= 0) {
        counter.set(0);
        indexObjectsPool.recycle(appendableIndex);
      }
    }
}
