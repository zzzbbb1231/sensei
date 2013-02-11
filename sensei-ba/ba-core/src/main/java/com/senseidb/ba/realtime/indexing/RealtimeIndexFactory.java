package com.senseidb.ba.realtime.indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import proj.zoie.api.ZoieIndexReader;

import com.alibaba.fastjson.util.IdentityHashMap;
import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.management.directory.AbstractFakeZoie;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class RealtimeIndexFactory extends AbstractFakeZoie {
  private final Object lock = new Object();
  private volatile RealtimeSnapshotIndexSegment currentSnapshot;
  private SenseiIndexReaderDecorator indexDecorator;
  private volatile SegmentToZoieReaderAdapter segmentToZoieReaderAdapter;
  private Map<RealtimeSnapshotIndexSegment, AtomicInteger> counters = new ConcurrentHashMap<RealtimeSnapshotIndexSegment, AtomicInteger>();
  private final IndexConfig indexConfig;

  public RealtimeIndexFactory(SenseiIndexReaderDecorator indexDecorator, IndexConfig indexConfig) {
    this.indexDecorator = indexDecorator;
    this.indexConfig = indexConfig;
  }

  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    
    synchronized (lock) {
      if (segmentToZoieReaderAdapter == null) {
          return Collections.EMPTY_LIST;
      }
      AtomicInteger atomicInteger = counters.get(segmentToZoieReaderAdapter.getOfflineSegment());
      if (atomicInteger == null) {
        //System.out.println("!!!Error the counter for the segment " + segmentToZoieReaderAdapter.getOfflineSegment() + " doesn't exist" + " time =" + System.currentTimeMillis());
      }
      int incrementAndGet = atomicInteger.incrementAndGet();
      //System.out.println("!!!incrementAndGet = " + incrementAndGet + "with segment " + segmentToZoieReaderAdapter.getOfflineSegment()  + " time =" + System.currentTimeMillis());
      //System.out.flush();
      RealtimeSnapshotIndexSegment indexSegment = (RealtimeSnapshotIndexSegment) segmentToZoieReaderAdapter.getOfflineSegment();
      for (String column : indexSegment.getColumnTypes().keySet())  {
        indexSegment.getForwardIndex(column).getDictionarySnapshot().getResurrectingMarker().incRef();
      }
      indexSegment.getReferencedSegment().getSegmentResurrectingMarker().incRef();
      return Arrays.asList((ZoieIndexReader<BoboIndexReader>) segmentToZoieReaderAdapter);
    }
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    synchronized (lock) {
      for (ZoieIndexReader<BoboIndexReader> reader : r) {
        SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) reader;
        RealtimeSnapshotIndexSegment indexSegment = (RealtimeSnapshotIndexSegment) adapter.getOfflineSegment();
        for (String column : indexSegment.getColumnTypes().keySet())  {
          indexSegment.getForwardIndex(column).getDictionarySnapshot().getResurrectingMarker().decRef();
        }
        indexSegment.getReferencedSegment().getSegmentResurrectingMarker().decRef();
        decrementCount(adapter);
      }
    }

  }

  public void decrementCount(SegmentToZoieReaderAdapter adapter) {
    IndexSegment offlineSegment = adapter.getOfflineSegment();
    AtomicInteger counter = counters.get(offlineSegment);

    if (counter == null) {
     // System.out.println("!!!Error" + "with segment " + segmentToZoieReaderAdapter.getOfflineSegment() +   " time =" + System.currentTimeMillis());
      return;
    }
    int count = counter.decrementAndGet();
    if (count == 0) {
      RealtimeSnapshotIndexSegment snapshot = (RealtimeSnapshotIndexSegment) offlineSegment;
      // we shouldn't recycle the snapshot if it's full, as it might be
      // referenced by the pendingIndexFactory
      if (!snapshot.isFull()) {
        // snapshot.recycle(indexConfig.getIndexObjectsPool());
      }
      counters.remove(snapshot);
    }
  }

  public void setSnapshot(RealtimeSnapshotIndexSegment newSnapshot) {
    synchronized (lock) {
      //System.out.println("set snapshot - " + newSnapshot.getLength());
      if (currentSnapshot == newSnapshot) {
        return;
      }
      if (counters.containsKey(newSnapshot)) {
        throw new IllegalStateException();
      }
      for (String column : newSnapshot.getColumnTypes().keySet())  {
          DictionarySnapshot newDictSnapshot = newSnapshot.getForwardIndex(column).getDictionarySnapshot();
          
          newDictSnapshot.getResurrectingMarker().incRef();
          if (currentSnapshot != null ) {
              currentSnapshot.getForwardIndex(column).getDictionarySnapshot().getResurrectingMarker().decRef();
          }
      } 
      
      if (currentSnapshot != null && newSnapshot.getReferencedSegment() != currentSnapshot.getReferencedSegment()) {
        currentSnapshot.setFull(true);
      }
      if (segmentToZoieReaderAdapter != null && segmentToZoieReaderAdapter.getOfflineSegment() != currentSnapshot) {
        throw new IllegalStateException();
      }
      if (segmentToZoieReaderAdapter != null) {
        decrementCount(segmentToZoieReaderAdapter);
      }
      currentSnapshot = newSnapshot;

      try {
        //System.out.println("!!!!Set the snapshot - " + newSnapshot + " time =" + System.currentTimeMillis());
        segmentToZoieReaderAdapter = new SegmentToZoieReaderAdapter(newSnapshot, "realtimeSegment", indexDecorator);
        if (counters.containsKey(newSnapshot)) {
          throw new IllegalStateException();
        }
        counters.put(newSnapshot, new AtomicInteger(1));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
     // System.out.flush();
    }
  }

  public Object getLock() {
    return lock;
  }

}
