package com.senseidb.ba.realtime.indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.management.directory.AbstractFakeZoie;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class RealtimeIndexFactory  extends AbstractFakeZoie {
    private final Object lock = new Object();
    private RealtimeSnapshotIndexSegment currentSnapshot;
    private SenseiIndexReaderDecorator indexDecorator;
    private SegmentToZoieReaderAdapter segmentToZoieReaderAdapter;
    private Map<RealtimeSnapshotIndexSegment, AtomicInteger> counters = new HashMap<RealtimeSnapshotIndexSegment, AtomicInteger>();
    private final IndexConfig indexConfig;
    public RealtimeIndexFactory(SenseiIndexReaderDecorator indexDecorator, IndexConfig indexConfig) {
        this.indexDecorator = indexDecorator;
        this.indexConfig = indexConfig;
    }
    @Override
    public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
        synchronized(lock) {
          counters.get(segmentToZoieReaderAdapter.getOfflineSegment()).incrementAndGet();
          return Arrays.asList((ZoieIndexReader<BoboIndexReader>)segmentToZoieReaderAdapter);
        }
    }

    @Override
    public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
      synchronized(lock) {
        for (ZoieIndexReader<BoboIndexReader> reader : r) {
          SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) reader;
          int count = counters.get(adapter.getOfflineSegment()).decrementAndGet();
          if (count == 0) {
            RealtimeSnapshotIndexSegment snapshot = (RealtimeSnapshotIndexSegment) adapter.getOfflineSegment();
            //we shouldn't recycle the snapshot if it's full, as it might be referenced by the pendingIndexFactory
            if (!snapshot.isFull()) {             
              //snapshot.recycle(indexConfig.getIndexObjectsPool());
            }
            counters.remove(snapshot);
          }
        }
      }
        
    }
   public void setSnapshot(RealtimeSnapshotIndexSegment newSnapshot) {
       if (currentSnapshot == newSnapshot) {
           return;
       }
       synchronized(lock) {
           if (currentSnapshot != null && newSnapshot.getReferencedSegment() != currentSnapshot.getReferencedSegment()) {
             currentSnapshot.setFull(true);
           } else  if (segmentToZoieReaderAdapter != null) {
             returnIndexReaders(Arrays.asList((ZoieIndexReader<BoboIndexReader>)segmentToZoieReaderAdapter));
           }
           currentSnapshot = newSnapshot;
           
           try {
             counters.put(newSnapshot, new AtomicInteger(1));
             segmentToZoieReaderAdapter = new SegmentToZoieReaderAdapter(currentSnapshot, "realtimeSegment", indexDecorator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
       }
   }
  public Object getLock() {
    return lock;
  }
  
}
