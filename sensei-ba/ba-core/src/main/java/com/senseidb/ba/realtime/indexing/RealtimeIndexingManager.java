package com.senseidb.ba.realtime.indexing;

import org.apache.log4j.Logger;

import proj.zoie.api.ZoieSegmentReader;

import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.SegmentAppendableIndex;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.domain.primitives.FieldRealtimeIndex;
import com.senseidb.ba.realtime.scheduler.SnapshotRefreshScheduler;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

public class RealtimeIndexingManager {
  private static Logger logger = Logger.getLogger(RealtimeIndexingManager.class);  
  private static final Counter currentNumberDocsInMemory = Metrics.newCounter(RealtimeIndexingManager.class, "currentNumberDocsInMemory");
    private volatile SegmentAppendableIndex currentIndex;
    private volatile RealtimeSnapshotIndexSegment snapshot;
    
    private SnapshotRefreshScheduler snapshotRefreshScheduler;
    private RealtimeDataProvider dataProvider;
    private IndexingCoordinator indexingCoordinator;
    private volatile boolean stopped = false;
    private IndexConfig indexConfig;

    private ShardingStrategy shardingStrategy;
    public void init(IndexConfig indexConfig, RealtimeDataProvider dataProvider, IndexingCoordinator indexingCoordinator, ShardingStrategy shardingStrategy) {
      this.indexConfig = indexConfig;    
      this.dataProvider = dataProvider;
      this.indexingCoordinator = indexingCoordinator;
      this.shardingStrategy = shardingStrategy;
   }
    
    public void start() {
      currentIndex = indexConfig.getIndexObjectsPool().getAppendableIndex();
      
      snapshotRefreshScheduler = new SnapshotRefreshScheduler() {
        @Override
        public int refresh() {
          synchronized(RealtimeIndexingManager.this) {         
            snapshot = currentIndex.refreshSearchSnapshot(indexConfig.getIndexObjectsPool());
            if (currentNumberDocsInMemory.count() != snapshot.getLength()) {
              currentNumberDocsInMemory.clear();
              currentNumberDocsInMemory.inc(snapshot.getLength());
            }
            indexingCoordinator.segmentSnapshotRefreshed(snapshot);
          }
          return snapshot.getLength();
        }
      };
      snapshotRefreshScheduler.init(indexConfig.getBufferSize(), indexConfig.getCapacity(), indexConfig.getRefreshTime());
      snapshotRefreshScheduler.start();
      indexingThread.setDaemon(true);
      indexingThread.start();
    }
    private Thread indexingThread = new Thread() {
      public void run() {
        while (!stopped) {
          try {           
            DataWithVersion next = dataProvider.next();
            
            if (next == null) {
              Thread.sleep(100L);
              continue;
            }
            
            if (shardingStrategy.calculateShard(next) != indexConfig.getPartition()) {
              continue;
            }            
            
            boolean isFull = currentIndex.add(next.getValues(), next.getVersion());
            
            if (isFull) {
              synchronized(RealtimeIndexingManager.this) {
                retireAndCreateNewSegment();
                RealtimeIndexingManager.this.wait();
              }
            } else {
              
              snapshotRefreshScheduler.sizeUpdated(currentIndex.getCurrenIndex());
            }
          } catch (Exception ex) {
            logger.error("error in indexing thread", ex);
            if (ex instanceof InterruptedException) {
              return;
            }
          }
        }
        
      };
    };
    
    
    public void stop() {
      notifySegmentPersisted();
      snapshotRefreshScheduler.stop();
      stopped = true;
      try {
        indexingThread.join();
      } catch (InterruptedException e) {
       throw new RuntimeException(e);
      }
    }

    protected void retireAndCreateNewSegment() {
      
      SegmentAppendableIndex appendableIndex = indexConfig.getIndexObjectsPool().getAppendableIndex();
     
      RealtimeSnapshotIndexSegment refreshedSearchSnapshot = currentIndex.refreshSearchSnapshot(indexConfig.getIndexObjectsPool());
      indexingCoordinator.segmentFullAndNewCreated(refreshedSearchSnapshot, currentIndex, appendableIndex);
      currentIndex = appendableIndex;
      
    }

    public RealtimeDataProvider getDataProvider() {
      return dataProvider;
    }

    public void notifySegmentPersisted() {
      synchronized(this) {
        this.notifyAll();
      }
    }

   
    
}
