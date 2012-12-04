package com.senseidb.ba.management;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

public class SegmentUtils {
  private static Logger logger = Logger.getLogger(SegmentUtils.class);
  public static void addToActiveSegments(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String  path = getActiveSegmentsPath(clusterName, partition, segmentId);
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
  }
  public static void removeFromActiveSegments(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String  path = getActiveSegmentsPath(clusterName, partition, segmentId);
    if (zkClient.exists(path)) {
      zkClient.deleteRecursive(path);
    }
  }
  public static String getActiveSegmentsPathForPartition(String clusterName, int partition) {
    return getActiveSegmentsPath(clusterName) +  "/" + partition;
  }
  public static String getActiveSegmentsPath(String clusterName) {
    return "/sensei-ba/" + clusterName + "/activeSegments";
  }
  public static String getActiveSegmentsPath(String clusterName, int partition, String segmentId) {
    return getActiveSegmentsPath(clusterName) + "/"+ + partition + "/" + segmentId;
  }
  public static String getSegmentInfoPath(String clusterName,  String segmentId) {
    return "/sensei-ba/" + clusterName + "/segmentInfo/" +  segmentId;
  }
  public static void registerAsActiveSegment(ZkClient zkClient, String clusterName, int partition, String segmentId) {
    String activeSegmentsPath = getActiveSegmentsPath(clusterName, partition, segmentId);
     if (!zkClient.exists(activeSegmentsPath)) {
       zkClient.createPersistent(activeSegmentsPath);
     }
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
}
