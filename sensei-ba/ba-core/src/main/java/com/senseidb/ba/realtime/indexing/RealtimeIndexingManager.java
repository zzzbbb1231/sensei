package com.senseidb.ba.realtime.indexing;

import org.apache.log4j.Logger;

import com.senseidb.ba.realtime.AppendableIndexPool;
import com.senseidb.ba.realtime.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.SegmentAppendableIndex;
import com.senseidb.ba.realtime.scheduler.SnapshotRefreshScheduler;

public class RealtimeIndexingManager {
  private static Logger logger = Logger.getLogger(RealtimeIndexingManager.class);  
  private Schema schema;
    private int capacity;
    private volatile SegmentAppendableIndex currentIndex;
    private volatile RealtimeSnapshotIndexSegment snapshot;
    
    private long refreshTime;
    private int bufferSize = 0;
   
    private long lastRefreshTime;
    private AppendableIndexPool appendableIndexPool;
    private SnapshotRefreshScheduler snapshotRefreshScheduler;
    private RealtimeDataProvider dataProvider;
    private IndexingCoordinator indexingCoordinator;
    public void init(Schema schema, int capacity, RealtimeDataProvider dataProvider) {
      this.schema = schema;
      this.capacity = capacity;
      this.dataProvider = dataProvider;
      appendableIndexPool = new AppendableIndexPool();
      appendableIndexPool.init(schema, capacity);
    }
    
    public void start() {
      currentIndex = appendableIndexPool.getAppendableIndex();
      snapshotRefreshScheduler = new SnapshotRefreshScheduler() {
        @Override
        public int refresh() {
          snapshot = currentIndex.getSearchSnapshot();
          indexingCoordinator.segmentSnapshotRefreshed(snapshot);
          return snapshot.getLength();
        }
      };
    }
    private Thread indexingThread = new Thread() {
      public void run() {
        while (true) {
          try {
            DataWithVersion next = dataProvider.next();
            if (next == null) {
              Thread.sleep(100L);
              continue;
            }
            boolean isFull = currentIndex.add(next.getValues(), next.getVersion());
            if (isFull) {
              retireAndCreateNewSegment();
            } else {
              snapshotRefreshScheduler.sizeUpdated(currentIndex.getCurrenIndex());
            }
          } catch (Exception ex) {
            logger.error("error in indexing thread", ex);
          }
        }
        
      };
    };
    
    
    public void stop() {
      snapshotRefreshScheduler.stop();
      
    }

    protected void retireAndCreateNewSegment() {
      
      SegmentAppendableIndex appendableIndex = appendableIndexPool.getAppendableIndex();
      currentIndex = appendableIndex;
      indexingCoordinator.segmentRetiredAndNewCreated(currentIndex.getSearchSnapshot(), currentIndex, appendableIndex);
      
    }
}
