package com.senseidb.ba.management;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

public class SegmentUtils {
  private static Logger logger = Logger.getLogger(SegmentUtils.class);
  public static void addToActiveSegments(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String  path = getActiveSegmentsPath(clusterName, partition, segmentId);
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
  }
  public static Properties getMetadata(ZkClient zkClient, String clusterName, String segmentId) throws IOException {
    String segmentInfoPath = getSegmentInfoPath(clusterName,  segmentId);
    String metadataPath = segmentInfoPath + "/metadata";
    if (!zkClient.exists(metadataPath)) {
      return null;
    }
    byte[] data = zkClient.readData(metadataPath);
    Properties properties = new Properties();
    properties.load(new ByteArrayInputStream(data));
    return properties;
  }
  public static boolean  removeFromActiveSegments(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String  path = getActiveSegmentsPath(clusterName, partition, segmentId);
    if (zkClient.exists(path)) {
      zkClient.deleteRecursive(path);
      return true;
    } else {
      return false;
    }
  }
  public static String getActiveSegmentsPathForPartition(String clusterName, int partition) {
    return getActiveSegmentsPath(clusterName) +  "/" + partition;
  }
  public static String getActiveSegmentsPath(String clusterName) {
    return getZkRoot() + "/" + clusterName + "/activeSegments";
  }
  public static String getZkRoot() {
    return "/sensei-ba";
  }
  public static String getMasterZkPath(String clusterName) {
    return SegmentUtils.getZkRoot() + "/" + clusterName + "/masters";
  }
  public static String getRefreshMarkerPath(String clusterName) {
    return SegmentUtils.getZkRoot() + "/" + clusterName + "/refreshMarkers";
  }
  public static String getActiveSegmentsPath(String clusterName, int partition, String segmentId) {
    return getActiveSegmentsPath(clusterName) + "/"+ + partition + "/" + segmentId;
  }
  public static String getSegmentInfoPath(String clusterName,  String segmentId) {
    return getZkRoot() + "/" + clusterName + "/segmentInfo/" +  segmentId;
  }
  public static String getSegmentInfoPath(String clusterName) {
    return getZkRoot() + "/" + clusterName + "/segmentInfo";
  }
  public static void registerAsActiveSegment(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String activeSegmentsPath = getActiveSegmentsPath(clusterName, partition, segmentId);
     if (!zkClient.exists(activeSegmentsPath)) {
       zkClient.createPersistent(activeSegmentsPath);
     }
  }
  public static List<String> getClusterNames(ZkClient zkClient) {
      
     if (!zkClient.exists(getZkRoot())) {
       return Collections.EMPTY_LIST;
     }
     return zkClient.getChildren(getZkRoot());
  }
  public static boolean isSegmentInfoReady(ZkClient zkClient, String clusterName, String segmentId) {
    try {
      String zkPath = getSegmentInfoPath(clusterName, segmentId);
      if (!zkClient.exists(zkPath)) {
        return false;
      }
      String readyPath = zkPath + "/readyFlag";
      if (!zkClient.exists(readyPath)) {
        return false;
      }
    } catch (Exception ex) {
      logger.error(ex);
    }
    return true;
  }
  public static void moveSegment(ZkClient zkClient, String clusterName, String segmentId, int oldPartition, int newPartition) {
    SegmentInfo retrievedFromZookeeper = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segmentId);
    Assert.notNull(retrievedFromZookeeper);
    SegmentUtils.removeFromActiveSegments(zkClient, clusterName, oldPartition, segmentId);
    SegmentUtils.addToActiveSegments(zkClient, clusterName, newPartition, segmentId);
    
  }
}
