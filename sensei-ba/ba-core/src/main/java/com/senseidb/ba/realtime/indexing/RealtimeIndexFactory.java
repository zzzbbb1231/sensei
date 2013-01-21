package com.senseidb.ba.realtime.indexing;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.management.directory.AbstractFakeZoie;
import com.senseidb.ba.realtime.RealtimeSnapshotIndexSegment;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class RealtimeIndexFactory  extends AbstractFakeZoie {
    private final Object lock = new Object();
    private RealtimeSnapshotIndexSegment currentSnapshot;
    private SenseiIndexReaderDecorator indexDecorator;
    private SegmentToZoieReaderAdapter segmentToZoieReaderAdapter;
    public RealtimeIndexFactory(SenseiIndexReaderDecorator indexDecorator) {
        this.indexDecorator = indexDecorator;
    }
    @Override
    public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
        synchronized(lock) {
            return Arrays.asList((ZoieIndexReader<BoboIndexReader>)segmentToZoieReaderAdapter);
        }
    }

    @Override
    public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
        // TODO Auto-generated method stub
        
    }
   public void setSnapshot(RealtimeSnapshotIndexSegment newSnapshot) {
       if (currentSnapshot == newSnapshot) {
           return;
       }
       synchronized(lock) {
           currentSnapshot = newSnapshot;
           try {
            segmentToZoieReaderAdapter = new SegmentToZoieReaderAdapter(currentSnapshot, "realtimeSegment", indexDecorator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
       }
   }

}
