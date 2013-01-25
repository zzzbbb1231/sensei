package com.senseidb.ba.realtime.indexing;

import org.apache.log4j.Logger;

import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.SegmentAppendableIndex;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.scheduler.SnapshotRefreshScheduler;

public class RealtimeIndexingManager {
  private static Logger logger = Logger.getLogger(RealtimeIndexingManager.class);  
 
    private volatile SegmentAppendableIndex currentIndex;
    private volatile RealtimeSnapshotIndexSegment snapshot;
    
    
    private SnapshotRefreshScheduler snapshotRefreshScheduler;
    private RealtimeDataProvider dataProvider;
    private IndexingCoordinator indexingCoordinator;
    private volatile boolean stopped = false;
    private IndexConfig indexConfig;
    public void init(IndexConfig indexConfig, RealtimeDataProvider dataProvider, IndexingCoordinator indexingCoordinator) {
      this.indexConfig = indexConfig;    
      this.dataProvider = dataProvider;
      this.indexingCoordinator = indexingCoordinator;
   }
    
    public void start() {
      currentIndex = indexConfig.getIndexObjectsPool().getAppendableIndex();
      snapshotRefreshScheduler = new SnapshotRefreshScheduler() {
        @Override
        public int refresh() {
          snapshot = currentIndex.refreshSearchSnapshot();
          indexingCoordinator.segmentSnapshotRefreshed(snapshot);
          return snapshot.getLength();
        }
      };
      snapshotRefreshScheduler.init(indexConfig.getBufferSize(), indexConfig.getCapacity(), indexConfig.getRefreshTime());
      snapshotRefreshScheduler.start();
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
            boolean isFull = currentIndex.add(next.getValues(), next.getVersion());
            /*if (currentIndex.getCurrenIndex() %5000 == 0) {
              System.out.println(currentIndex.getCurrenIndex());
            }*/
            if (isFull) {
              retireAndCreateNewSegment();
            } else {
              snapshotRefreshScheduler.sizeUpdated(currentIndex.getCurrenIndex());
            }
          } catch (Throwable ex) {
            logger.error("error in indexing thread", ex);
          }
        }
        
      };
    };
    
    
    public void stop() {
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
      indexingCoordinator.segmentFullAndNewCreated(currentIndex.refreshSearchSnapshot(), currentIndex, appendableIndex);
      currentIndex = appendableIndex;
      
    }

    public RealtimeDataProvider getDataProvider() {
      return dataProvider;
    }
    
}
