package com.senseidb.ba.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;

public class ZkManager {
  private static Logger logger = Logger.getLogger(DirectoryBasedFactoryManager.class);    
  private ZkClient zkClient;
    private final String clusterName;
   
    public ZkManager(String zkString, String clusterName) {
       this.clusterName = clusterName;
      zkClient = new ZkClient(zkString);
      zkClient.setZkSerializer(new BytesPushThroughSerializer());
    }
    public ZkManager(ZkClient zkClient, String clusterName) {
      this.zkClient = zkClient;
      this.clusterName = clusterName;
    
   }
    public void registerSegment(int partition, String segmentId, String pathUrl, long timeCreated) {
      HashMap<String, String> config = new HashMap<String, String>();
      config.put("timeCreated", String.valueOf(timeCreated));
      registerSegment(partition, segmentId, pathUrl, config);
    }
    public void registerSegment(int partition, String segmentId, String pathUrl, Map<String, String> newConf) {
      registerSegment(clusterName, partition, segmentId, pathUrl, newConf);
    }
    public void registerSegment(String clusterName, int partition, String segmentId, String pathUrl, Map<String, String> newConf) {
      
      
      logger.info("Registering the new segment with id = " + segmentId + ", partition = " + partition + ", pathUrl = " + pathUrl);
      
      String partitionPath = SegmentUtils.getActiveSegmentsPathForPartition(clusterName, partition);
      if (!zkClient.exists(partitionPath )) {
        zkClient.createPersistent(partitionPath, true);
      }
      SegmentInfo segmentInfo;
      if (SegmentUtils.isSegmentInfoReady(zkClient, clusterName, segmentId)) {
         segmentInfo = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segmentId);
        
        if (!segmentInfo.getPathUrls().contains(pathUrl)) {
          segmentInfo.getPathUrls().add(pathUrl);
        }
        segmentInfo.getConfig().putAll(newConf);
      } else {
        List<String> pathUrls = new ArrayList<String>();
        pathUrls.add(pathUrl);
        segmentInfo = new SegmentInfo( segmentId, pathUrls, newConf);
      }
      segmentInfo.saveInfoToZookeeper(zkClient, clusterName);
      SegmentUtils.registerAsActiveSegment(zkClient, clusterName, partition, segmentId);
    }
    public void moveSegment(String segmentId, int oldPartition, int newPartition) {
      SegmentInfo retrievedFromZookeeper = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segmentId);
      Assert.notNull(retrievedFromZookeeper);
      SegmentUtils.removeFromActiveSegments(zkClient, clusterName, oldPartition, segmentId);
      SegmentUtils.addToActiveSegments(zkClient, clusterName, newPartition, segmentId);
      
    }
    
    public void removePartition(int partition) {
      String partitionPath = SegmentUtils.getActiveSegmentsPathForPartition(clusterName, partition);
      if (zkClient.exists(partitionPath)) {
        zkClient.deleteRecursive(partitionPath);
      } 
    }
   
    public List<String> getPartitions() {
      List<String> ret = new ArrayList<String>();
      String activeSegmentsPath = SegmentUtils.getActiveSegmentsPath(clusterName);
      if (!zkClient.exists(activeSegmentsPath)) {
        return Collections.EMPTY_LIST;
      }
      List<String> children = zkClient.getChildren(activeSegmentsPath);
      for (String partition : children) {
        ret.add(partition);
      }
      return ret;
    }
    public List<String> getSegmentsForPartition(String partition) {
        String segmentPath = SegmentUtils.getActiveSegmentsPathForPartition(clusterName, Integer.parseInt(partition));
        if (!zkClient.exists(segmentPath)) {
          return Collections.EMPTY_LIST;
        }
        List<String> children = zkClient.getChildren(segmentPath);
        return children;
    }
    public SegmentInfo getSegmentInfo(String segmentId) {
      return SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segmentId);
    }
    
    public boolean removeSegment(int partition, String segmentId) {
      String segmentPath = SegmentUtils.getActiveSegmentsPath(clusterName, partition, segmentId);
      if (zkClient.exists(segmentPath)) {
        zkClient.deleteRecursive(segmentPath);
        return true;
      }
    return false;
    }
    public boolean removeSegment(String segmentPath) {
      if (zkClient.exists(segmentPath)) {
        zkClient.deleteRecursive(segmentPath);
        return true;
      }
    return false;
    }
    public ZkClient getZkClient() {
      return zkClient;
    }
    public void setZkClient(ZkClient zkClient) {
      this.zkClient = zkClient;
    }
    public String getClusterName() {
      return clusterName;
    }
    
    
}
