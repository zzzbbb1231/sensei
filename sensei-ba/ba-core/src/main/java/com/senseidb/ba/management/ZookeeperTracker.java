package com.senseidb.ba.management;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public  class ZookeeperTracker implements IZkChildListener {
  private ZkClient zkClient;
  public static String ZK_BASE_PATH =  "/sensei-ba/partitions";
  private String partitionPath;
  
  private final SegmentTracker segmentTracker;
  public ZookeeperTracker(ZkClient zkClient, int partitionId, SegmentTracker segmentTracker) {
    this.zkClient = zkClient;
    this.segmentTracker = segmentTracker;
    zkClient.setZkSerializer(new BytesPushThroughSerializer());
    partitionPath = ZK_BASE_PATH + "/" + partitionId;
  }
  public void start() {
    if (!zkClient.exists(partitionPath)) {
      zkClient.createPersistent(partitionPath, true);
    }
    handleChildChange(partitionPath, zkClient.getChildren(partitionPath));
    zkClient.subscribeChildChanges(partitionPath, this);
  }
  public void stop() {
    zkClient.unsubscribeChildChanges(partitionPath, this);

  }
  @Override
  public  void handleChildChange(String parentPath, List<String> currentChilds)  {
    if (currentChilds == null) {
      //deleted
      currentChilds = Collections.EMPTY_LIST;
    }
    Set<String> toDelete = null;
    Map<String, SegmentInfo> toAdd = null;
    Set<String> currentSegments = new HashSet<String>(segmentTracker.getActiveSegments());
    currentSegments.addAll(segmentTracker.getLoadingSegments());
    Set<String> childs = new HashSet<String>(currentChilds);
    synchronized(segmentTracker.globalLock) {
      for (String child : childs) {
        if (!currentSegments.contains(child)) {
          if (toAdd == null) toAdd = new HashMap<String, SegmentInfo>();
          toAdd.put(child, SegmentInfo.fromBytes((byte[])zkClient.readData(partitionPath + "/" + child)));
        }
      }
      for (String existingSegment : currentSegments) {
        if (!childs.contains(existingSegment)) {
          if (toDelete == null) toDelete = new HashSet<String>();
          toDelete.add(existingSegment);
        }
      }
    }
  
    if (toAdd != null) {
      for (String newSegment : toAdd.keySet()) {
        segmentTracker.addSegment(newSegment, toAdd.get(newSegment));        
      }
    }
    if (toDelete != null) {
      for (String oldSegment : toDelete) {
        segmentTracker.removeSegment(oldSegment);
      }
    }
  }
}
