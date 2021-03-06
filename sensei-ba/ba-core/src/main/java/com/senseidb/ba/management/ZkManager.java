package com.senseidb.ba.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.ba.util.TimerService;

public class ZkManager {
  private static Logger logger = Logger.getLogger(ZkManager.class);    
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
    public void registerSegment(final String clusterName, int partition, String segmentId, String pathUrl, Map<String, String> newConf) {
      
      
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
        String oldCrc = segmentInfo.getConfig().get(SegmentMetadata.SEGMENT_CRC);
        String newCrc = newConf.get(SegmentMetadata.SEGMENT_CRC);
        segmentInfo.getConfig().putAll(newConf);
        segmentInfo.getConfig().put("overriten", String.valueOf(System.currentTimeMillis()));
        if ((oldCrc == null && newCrc != null) || (oldCrc != null && !oldCrc.equals(newCrc))) {
          logger.info("Trying to override the existing segment");
          String markerPath = SegmentUtils.getRefreshMarkerPath(clusterName) + "/" + System.currentTimeMillis() + "/" + segmentId;
          if (!zkClient.exists(markerPath)) {
            zkClient.createPersistent(markerPath, true);
            TimerService.timer.schedule(new TimerTask() {
              @Override
              public void run() {
                long time = System.currentTimeMillis();
                List<String> children = zkClient.getChildren(SegmentUtils.getRefreshMarkerPath(clusterName));
                for (String child : children) {
                  if (child == null) {
                    continue;
                  }
                  if (time - Long.parseLong(child) > 4000) {
                    zkClient.deleteRecursive(SegmentUtils.getRefreshMarkerPath(clusterName) + "/" + child);
                  }
                }
              }
            }, 5000);
           
          }
        }
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
      String infoPath = SegmentUtils.getSegmentInfoPath(clusterName); 
      if (zkClient.exists(infoPath)) {
        zkClient.deleteRecursive(infoPath);
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
      String segmentInfoPath = SegmentUtils.getSegmentInfoPath(clusterName, segmentId);
      boolean ret = false;
      if (zkClient.exists(segmentPath)) {
        zkClient.deleteRecursive(segmentPath); 
        ret = true;
      }
      if (zkClient.exists(segmentInfoPath)) {
        zkClient.deleteRecursive(segmentInfoPath);        
      }
    return ret;
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
