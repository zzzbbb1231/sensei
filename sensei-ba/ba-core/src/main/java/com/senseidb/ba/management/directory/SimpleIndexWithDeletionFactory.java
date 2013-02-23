package com.senseidb.ba.management.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

public class SimpleIndexWithDeletionFactory extends AbstractFakeZoie {

  private final Object globalLock = new Object();;
  private Map<SegmentToZoieReaderAdapter, AtomicInteger> counters = new HashMap<SegmentToZoieReaderAdapter, AtomicInteger>();
  private GazelleSegmentDeletionListener deletionListener;

  @SuppressWarnings("rawtypes")
  public SimpleIndexWithDeletionFactory() {
  }

  public void addSegment(SegmentToZoieReaderAdapter indexSegment) {
    synchronized (globalLock) {
      counters.put(indexSegment, new AtomicInteger(1));
    }
  }

  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() {
    synchronized (globalLock) {
      List<ZoieIndexReader<BoboIndexReader>> ret = new ArrayList<ZoieIndexReader<BoboIndexReader>>(counters.size());
      for (SegmentToZoieReaderAdapter adapter : counters.keySet()) {
        ret.add(adapter);
        counters.get(adapter).incrementAndGet();
      }
      return ret;
    }
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    synchronized (globalLock) {
      for (ZoieIndexReader<BoboIndexReader> reader : r) {
        SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) reader;
        int count = counters.get(adapter).decrementAndGet();
        if (count == 0) {
          counters.remove(adapter);
          if (deletionListener != null) {
            if (deletionListener != null) {
              deletionListener.onDelete(adapter);
            }
          }

        }
      }
    }
  }

  public void removeSegment(SegmentToZoieReaderAdapter indexSegment) {
    returnIndexReaders(Arrays.asList((ZoieIndexReader<BoboIndexReader>) indexSegment));
  }

  public Object getGlobalLock() {
    return globalLock;
  }

  
  public GazelleSegmentDeletionListener getDeletionListener() {
    return deletionListener;
  }

  public void setDeletionListener(GazelleSegmentDeletionListener deletionListener) {
    this.deletionListener = deletionListener;
  }


  public static interface GazelleSegmentDeletionListener {
    public void onDelete(ZoieIndexReader<BoboIndexReader> reader);
  }
}
