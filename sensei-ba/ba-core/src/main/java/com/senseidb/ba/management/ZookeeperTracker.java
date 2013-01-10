package com.senseidb.ba.management;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;



public  class ZookeeperTracker implements IZkChildListener, IZkDataListener, SegmentLoaderListener {
  private static Logger logger =Logger.getLogger(ZookeeperTracker.class);
  private ZkClient zkClient;
  private String partitionPath;
  private static final Counter segmentsFailedToLoad = Metrics.newCounter(SegmentTracker.class, "segmentsFailedToLoad");
  private final SegmentTracker segmentTracker;
  private final String clusterName;
  private final int partitionId;
  private final int nodeId;
  public ZookeeperTracker(ZkClient zkClient, String clusterName, int nodeId, int partitionId, SegmentTracker segmentTracker) {
    this.zkClient = zkClient;
    this.clusterName = clusterName;
    this.nodeId = nodeId;
    this.partitionId = partitionId;
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
        segmentTracker.addSegment(newSegment, toAdd.get(newSegment), this);        
      }
    }
    if (toDelete != null) {
      for (String oldSegment : toDelete) {
        segmentTracker.removeSegment(oldSegment);
      }
    }
  }
  public synchronized void segmentLoadedSuccsfully(String segmentId) {
    if (failedSegments.containsKey(segmentId)) {
      logger.info("Segment " + segmentId + "recovered succesfully. Removing specific recovering logic");
      
      failedSegments.remove(segmentId);
      segmentsFailedToLoad.clear();
      segmentsFailedToLoad.inc(failedSegments.size());
      try {
      zkClient.unsubscribeDataChanges(getMetadataPath(segmentId), this);
      } catch (Exception ex) {
        logger.warn(ex.getMessage(), ex);
      }
    }
  }
   private Map<String, String> failedSegments = new ConcurrentHashMap<String, String>();
   
  public void segmentFailedToLoad(String segmentId) {
    logger.warn("The segment " + segmentId + " has not been loaded succesfully");
    if (failedSegments.containsKey(segmentId)) {
      logger.info("The segment " + segmentId + " is already marked as failed. Do not need to do anything");
      return;
    }
    failedSegments.put(segmentId, segmentId);
    segmentsFailedToLoad.clear();
    segmentsFailedToLoad.inc(failedSegments.size());
    logger.info("Subscribing to the data changes for the segment - " + segmentId);
    zkClient.subscribeDataChanges(getMetadataPath(segmentId), this);
  }
  public String getMetadataPath(String segmentId) {
    return SegmentUtils.getSegmentInfoPath(clusterName, segmentId) + "/metadata";
  }
  
  @Override
  public void handleDataChange(String dataPath, Object data) throws Exception {
   if (dataPath == null || data == null) {
     return;
   }
   String segmentId = extractSegmentId(dataPath);
   if (!failedSegments.containsKey(segmentId)) {
     logger.info("The segment " + segmentId + " is not marked as failed any more");
     segmentLoadedSuccsfully(segmentId);
     return;
   }
   synchronized(segmentTracker) {
     if (segmentTracker.getActiveSegments().contains(segmentId)) {
       segmentLoadedSuccsfully(segmentId);
       return;
     }
   }
   logger.info("The segment " + segmentId + " was changed. Trying to register it again");
   SegmentInfo segmentInfo = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segmentId);
   segmentTracker.addSegment(segmentId, segmentInfo, this);
  }
  @Override
  public void handleDataDeleted(String dataPath) throws Exception {
    String segmentId = extractSegmentId(dataPath);
    logger.info("Removing segment - " + segmentId);
    failedSegments.remove(segmentId);
    
  }
  private String extractSegmentId(String dataPath) {
    if (dataPath.endsWith("/")) {
      dataPath = dataPath.substring(0, dataPath.length() - 1);
    }
    if (!dataPath.endsWith("/metadata")) {
      throw new IllegalStateException("The dataPath " + dataPath + " should end with /metadata");
    }
    dataPath = dataPath.substring(0,  dataPath.length() - "/metadata".length());
    String ret =  dataPath.substring(dataPath.lastIndexOf("/") + 1);
    return ret;
  }
  
}
