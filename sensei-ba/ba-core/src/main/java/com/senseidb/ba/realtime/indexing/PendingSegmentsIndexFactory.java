package com.senseidb.ba.realtime.indexing;

import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.SortUtil;
import com.senseidb.ba.management.directory.AbstractFakeZoie;
import com.senseidb.ba.realtime.SegmentAppendableIndex;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.domain.primitives.FieldRealtimeIndex;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class PendingSegmentsIndexFactory  extends AbstractFakeZoie {
  private final Object lock = new Object();
  private static Logger logger = Logger.getLogger(PendingSegmentsIndexFactory.class);  
  
  private LinkedBlockingQueue<SegmentAppendableIndex> segmentQueue = new LinkedBlockingQueue<SegmentAppendableIndex>();
  private Map<SegmentAppendableIndex, SegmentToZoieReaderAdapter<BoboIndexReader>> zoieSegments = new HashMap<SegmentAppendableIndex, SegmentToZoieReaderAdapter<BoboIndexReader>>();
  private Map<SegmentToZoieReaderAdapter, AtomicInteger> counters = new HashMap<SegmentToZoieReaderAdapter, AtomicInteger>();
  private IndexConfig indexConfig;
  private final SegmentPersistedListener listener;
  public PendingSegmentsIndexFactory(SegmentPersistedListener listener, IndexConfig indexConfig) {
    this.listener = listener;
    this.indexConfig = indexConfig;
  }
  public void start() {
    persistingThread.start();
  }
  public void stop() {
    isStoppedFlag = true;
    try {
      persistingThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  public void addSegment(SegmentAppendableIndex appendableIndex, RealtimeSnapshotIndexSegment indexSegment, SenseiIndexReaderDecorator decorator) {
    try {
    synchronized(lock) {
      SegmentToZoieReaderAdapter<BoboIndexReader> adapter = new SegmentToZoieReaderAdapter<BoboIndexReader>(indexSegment, appendableIndex.getName(), decorator);
      zoieSegments.put(appendableIndex, adapter);
      counters.put(adapter, new AtomicInteger(1));
    }
    } catch (Exception ex) {
      throw new RuntimeException("Should never happen", ex);
    }
    segmentQueue.add(appendableIndex);
  }
  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    synchronized(lock) {
      for (SegmentToZoieReaderAdapter index : zoieSegments.values()) {
        counters.get(index).incrementAndGet();
      }
      return (List<ZoieIndexReader<BoboIndexReader>>) new ArrayList(zoieSegments.values());
    }
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    List<SegmentAppendableIndex> toRemove = null;
    List<SegmentToZoieReaderAdapter> adaptersToRemove = null;
    synchronized(lock) {
    for (ZoieIndexReader<BoboIndexReader> indexReader : r ) {
      SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) indexReader;
      SegmentAppendableIndex referencedSegment = ((RealtimeSnapshotIndexSegment)adapter.getOfflineSegment()).getReferencedSegment();
      AtomicInteger counter = counters.get(adapter);
      if(counter.decrementAndGet() == 0) {
        if (toRemove == null) {
          toRemove = new ArrayList<SegmentAppendableIndex>();
          
        }
        if (adaptersToRemove == null) {
          adaptersToRemove = new ArrayList<SegmentToZoieReaderAdapter>();
        }
        toRemove.add(referencedSegment);
        adaptersToRemove.add(adapter);
      }
    }  
    if (adaptersToRemove != null) {
      counters.keySet().removeAll(adaptersToRemove);
    }
    //ZoieSegments are removed in persisting thread
    if (toRemove != null) {
      for (SegmentAppendableIndex segment : toRemove) {
      indexConfig.getIndexObjectsPool().recycle(segment);
    }
    }
    }
    
  }

  public Object getLock() {
    return lock;
  }
  
  private Map<SegmentAppendableIndex, IndexSegment> persistedSegments = new ConcurrentHashMap<SegmentAppendableIndex, IndexSegment>();
  private boolean isStoppedFlag = false;
  private Thread persistingThread = new Thread() {
    public void run() {
      while (!isStoppedFlag) {
        try {
          SegmentAppendableIndex segmentToProcess = segmentQueue.poll();
          if (segmentToProcess == null) {
            continue;
          }
          long time = System.currentTimeMillis();
          GazelleIndexSegmentImpl persistedSegment = sortAndPersistSegment(segmentToProcess.refreshSearchSnapshot(indexConfig.getIndexObjectsPool()));
          logger.info("The segment has been persisted and sorted in " + (System.currentTimeMillis() - time) + " ms");
          if (persistedSegment == null) {
            continue;
          }
          listener.getMetadata().update(segmentToProcess.getVersion());
          synchronized (lock) {
            SegmentToZoieReaderAdapter<BoboIndexReader> segmentAdapter = zoieSegments.remove(segmentToProcess);
            listener.onSegmentPersisted(segmentToProcess, persistedSegment);
            returnIndexReaders(Arrays.asList((ZoieIndexReader<BoboIndexReader>)segmentAdapter));
          }          
        } catch (Exception ex) {
          logger.error("error in indexing thread", ex);
        }
      }
      
    };
  };

  protected GazelleIndexSegmentImpl sortAndPersistSegment(RealtimeSnapshotIndexSegment segmentToProcess) {
    GazelleIndexSegmentImpl gazelleIndexSegmentImpl = new GazelleIndexSegmentImpl();
    
    final ColumnSearchSnapshot[] indexes = new ColumnSearchSnapshot[indexConfig.getSortedColumns().length]; 
    for (int i = 0; i <  indexConfig.getSortedColumns().length; i++) {
      indexes[i] = (ColumnSearchSnapshot) segmentToProcess.getForwardIndex(indexConfig.getSortedColumns()[i].trim());
    }
    
    final int[] permArray = indexConfig.getIndexObjectsPool().getPermArray();
    try {
    for (int i = 0; i < permArray.length; i++) {
      permArray[i] = i;
    }
   
    SortUtil.quickSort(0, permArray.length, new SortUtil.IntComparator() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }
      @Override
      public int compare(int k1, int k2) {
        
        for (ColumnSearchSnapshot<int[]> index :  indexes) {
          IntList invPermutationArray = index.getDictionarySnapshot().getInvPermutationArray();
          int val1 = invPermutationArray.getInt(index.getForwardIndex()[permArray[k1]]);
          int val2 = invPermutationArray.getInt(index.getForwardIndex()[permArray[k2]]);
          if (val1 > val2) return 1;
          if (val2 > val1) return -1;
        }
        return 0; 
      }
    }, new SortUtil.Swapper() {
      @Override
      public void swap(int a, int b) {
        int tmp = permArray[b];
        permArray[b] = permArray[a];
        permArray[a] = tmp;
      }
    });
    for (String columnName : segmentToProcess.getColumnTypes().keySet()) {
      ColumnSearchSnapshot searchSnapshot = (ColumnSearchSnapshot) segmentToProcess.getForwardIndex(columnName);
     /* for (int i = 0; i < searchSnapshot.getForwardIndexSize(); i++) {
        int swapValue = permArray[i];
        if (i < swapValue) {
          int tmp = forwardIndex[swapValue];
          forwardIndex[swapValue] = forwardIndex[i];
          forwardIndex[i] = tmp;
        }
        
        forwardIndex[i] = permutationArray.getInt(forwardIndex[i]);
      }*/
    
      ForwardIndex builtIndex = OnePassIndexCreator.build(searchSnapshot, permArray, searchSnapshot.getDictionarySnapshot(), searchSnapshot.getDictionarySnapshot().produceDictionary(), segmentToProcess.getColumnTypes().get(columnName), columnName, searchSnapshot.getForwardIndexSize());
      Assert.state(builtIndex.getLength() == searchSnapshot.getForwardIndexSize());
      gazelleIndexSegmentImpl.getForwardIndexes().put(columnName, builtIndex);
      gazelleIndexSegmentImpl.getDictionaries().put(columnName, builtIndex.getDictionary());
      gazelleIndexSegmentImpl.getColumnTypes().put(columnName, builtIndex.getColumnType());
      gazelleIndexSegmentImpl.setLength(searchSnapshot.getForwardIndexSize());
      gazelleIndexSegmentImpl.getColumnMetadataMap().put(columnName, ((MetadataAware)builtIndex).getColumnMetadata());
    }
    File dir = new File(indexConfig.getIndexDir(), segmentToProcess.getReferencedSegment().getName());
    SegmentPersistentManager.flushToDisk(gazelleIndexSegmentImpl, dir);
    new File(dir, "finishedLoading").createNewFile();
   
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      throw new RuntimeException(ex);
    } finally {
      indexConfig.getIndexObjectsPool().recycle(permArray);
    }
    
    return gazelleIndexSegmentImpl;
  }
  public static interface SegmentPersistedListener {
    public void onSegmentPersisted(SegmentAppendableIndex segmentToProcess, GazelleIndexSegmentImpl persistedSegment);
    public Metadata getMetadata();
  }
  
}
