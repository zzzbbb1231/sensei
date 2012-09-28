package com.senseidb.ba.management;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public class ZkManager {
    private ZkClient zkClient;
    public ZkManager(String zkString) {
       zkClient = new ZkClient(zkString);
      zkClient.setZkSerializer(new BytesPushThroughSerializer());
    }
    public void registerSegment(int partition, String segmentId, String pathUrl, SegmentType type, long timeCreated, long timeToLive) {
      String partitionPath = ZookeeperTracker.ZK_BASE_PATH + "/" + partition;
      if (!zkClient.exists(partitionPath)) {
        zkClient.createPersistent(partitionPath, true);
      }
      SegmentInfo segmentInfo = new SegmentInfo(segmentId, pathUrl, type, timeCreated, timeToLive);
      zkClient.createPersistent(partitionPath + "/" + segmentId, segmentInfo.toByteArray());
    }
}
