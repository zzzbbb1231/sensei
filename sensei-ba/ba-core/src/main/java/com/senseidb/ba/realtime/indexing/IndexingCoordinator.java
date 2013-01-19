package com.senseidb.ba.realtime.indexing;

import com.senseidb.ba.realtime.AppendableIndexPool;
import com.senseidb.ba.realtime.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.SegmentAppendableIndex;

public class IndexingCoordinator {
  AppendableIndexPool appendableIndexPool;  
  public void segmentSnapshotRefreshed(RealtimeSnapshotIndexSegment indexSegment) {
      
    }
     public void segmentRetiredAndNewCreated(RealtimeSnapshotIndexSegment fullExisingSnapshot, SegmentAppendableIndex oldSegment, SegmentAppendableIndex newSegment) {
      
    }
    
}
