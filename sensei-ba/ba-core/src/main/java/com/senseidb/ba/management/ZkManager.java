package com.senseidb.ba.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;

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
    public void registerSegment(int partition, String segmentId, String pathUrl, SegmentType type, long timeCreated, long timeToLive) {
      logger.info("Registering the new segment with id = " + segmentId + ", partition = " + partition + ", pathUrl = " + pathUrl);
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName + "/" + partition;
      if (!zkClient.exists(partitionPath)) {
        zkClient.createPersistent(partitionPath, true);
      }
      
      if (zkClient.exists(partitionPath + "/" + segmentId)) {
        SegmentInfo oldSegmentInfo = SegmentInfo.fromBytes((byte[])zkClient.readData(partitionPath + "/" + segmentId));
        
        if (oldSegmentInfo.getPathUrl().contains(pathUrl)) {
          logger.info("The url - " + pathUrl + " is already registered for the segment - " + segmentId);
          //but still we need to recreate the segment
          pathUrl = oldSegmentInfo.getPathUrl();
        } else {
          pathUrl = oldSegmentInfo.getPathUrl() + "," + pathUrl;
        }
        
        timeCreated = oldSegmentInfo.getTimeCreated();
        removeSegment(partition, segmentId);
      }
      SegmentInfo segmentInfo = new SegmentInfo(segmentId, pathUrl, type, timeCreated, timeToLive);
      zkClient.createPersistent(partitionPath + "/" + segmentId, segmentInfo.toByteArray());
    }
    public boolean segmentExists(int partition, String segmentId) {
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName + "/" + partition;
      return zkClient.exists(partitionPath + "/" + segmentId);
    }
    public void removePartition(int partition) {
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName + "/" + partition;
      if (zkClient.exists(partitionPath)) {
        zkClient.deleteRecursive(partitionPath);
      } 
    }
    public List<String> getChildren() {
      List<String> ret = new ArrayList<String>();
      List<String> children = zkClient.getChildren(ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName);
      for (String partition : children) {
        for (String segment : zkClient.getChildren(ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName + "/" + partition)) {
          ret.add(partition + "/" + segment);
        }
      }
      return ret;
    }
    public List<String> getPartitions() {
      List<String> ret = new ArrayList<String>();
      if (!zkClient.exists(ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName)) {
        return Collections.EMPTY_LIST;
      }
      List<String> children = zkClient.getChildren(ZookeeperTracker.ZK_BASE_PATH + "/" + clusterName);
      for (String partition : children) {
        ret.add(partition);
      }
      return ret;
    }
    public List<String> getSegments(String partition) {
        String segmentPath =ZookeeperTracker.ZK_BASE_PATH + "/"  + clusterName + "/" + partition;
        if (!zkClient.exists(segmentPath)) {
          return Collections.EMPTY_LIST;
        }
        List<String> children = zkClient.getChildren(segmentPath);
        return children;
    }
    public SegmentInfo getSegmentInfo(String partition, String segmentId) {
      String segmentPath =ZookeeperTracker.ZK_BASE_PATH + "/"  + clusterName + "/" + partition  + "/" + segmentId;
      if (zkClient.exists(segmentPath)) {
        SegmentInfo segmentInfo = SegmentInfo.fromBytes((byte[])zkClient.readData(segmentPath));
        return segmentInfo;
      }
      return null;
    }
    
    
    public boolean removeSegment(int partition, String segmentId) {
      String segmentPath =ZookeeperTracker.ZK_BASE_PATH + "/"  + clusterName + "/" + partition  + "/" + segmentId;
      if (zkClient.exists(segmentPath)) {
        zkClient.deleteRecursive(segmentPath);
        return true;
      }
    return false;
    }
    public static void main(String[] args) {
      ZkClient zkClient = new ZkClient("localhost:2181");
      zkClient.deleteRecursive("/sensei-ba/partitions/testCluster2");
    }
}
