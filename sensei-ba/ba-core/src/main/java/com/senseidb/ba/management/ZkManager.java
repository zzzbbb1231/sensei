package com.senseidb.ba.management;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public class ZkManager {
    private ZkClient zkClient;
    public ZkManager(String zkString) {
       zkClient = new ZkClient(zkString);
      zkClient.setZkSerializer(new BytesPushThroughSerializer());
    }
    public ZkManager(ZkClient zkClient) {
      this.zkClient = zkClient;
    
   }
    public void registerSegment(int partition, String segmentId, String pathUrl, SegmentType type, long timeCreated, long timeToLive) {
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + partition;
      if (!zkClient.exists(partitionPath)) {
        zkClient.createPersistent(partitionPath, true);
      }
      SegmentInfo segmentInfo = new SegmentInfo(segmentId, pathUrl, type, timeCreated, timeToLive);
      zkClient.createPersistent(partitionPath + "/" + segmentId, segmentInfo.toByteArray());
    }
    public void removePartition(int partition) {
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + partition;
      if (zkClient.exists(partitionPath)) {
        zkClient.deleteRecursive(partitionPath);
      }
    
    }
    public void removeSegment(int partition, String segmentId) {
      String segmentPath =ZookeeperTracker.ZK_BASE_PATH + "/" + partition  + "/" + segmentId;
      if (zkClient.exists(segmentPath)) {
        zkClient.deleteRecursive(segmentPath);
      }
    
    }
}
