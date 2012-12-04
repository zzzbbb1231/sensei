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
import org.springframework.util.Assert;

public  class ZookeeperTracker implements IZkChildListener {
  private ZkClient zkClient;
  private String partitionPath;
  
  private final SegmentTracker segmentTracker;
  private final String clusterName;
  public ZookeeperTracker(ZkClient zkClient, String clusterName, int nodeId, int partitionId, SegmentTracker segmentTracker) {
    this.zkClient = zkClient;
    this.clusterName = clusterName;
    partitionPath = SegmentUtils.getActiveSegmentsPathForPartition(clusterName, partitionId);
    this.segmentTracker = segmentTracker;
    zkClient.setZkSerializer(new BytesPushThroughSerializer());
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
          SegmentInfo segmentInfo = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, child);
          Assert.notNull(segmentInfo, "Segment " + child + " was registered as the active segment, but the corresponding segmentInfo is not present in zookeeper");
          toAdd.put(child, segmentInfo);
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
