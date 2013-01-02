package com.senseidb.ba.management.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.ba.file.http.AllClustersRestSegmentServlet;
import com.senseidb.ba.file.http.RestSegmentServlet;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.ba.management.ZkManager;

public class MasterInfoServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(RestSegmentServlet.class);  
 
  private String clusterName;
  private ZkManager zkManager;
  private int maxPartition;
  
  @Override
  public void init(ServletConfig config) throws ServletException {   
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    clusterName = config.getInitParameter("clusterName");
    zkManager = new ZkManager(zkUrl, clusterName);
  
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      String pathInfo = req.getPathInfo();
      List<String> tokenized = AllClustersRestSegmentServlet.tokenizePathInfo(pathInfo);
      String clusterName = null;
      if (tokenized.size() > 0) {
        clusterName = tokenized.get(0);
        JSONObject ret = getClusterMasterInfo(clusterName);
        resp.getOutputStream().print( ret.toString(1));
      } else {
        JSONObject ret = new JSONObject();
        for (String clusterNameIt : SegmentUtils.getClusterNames(zkManager.getZkClient())) {
          ret.put(clusterNameIt, getClusterMasterInfo(clusterNameIt));
        }
        resp.getOutputStream().print( ret.toString(1));
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      resp.getOutputStream().flush();
    }
    
  }

  public JSONObject getClusterMasterInfo(String clusterName) throws JSONException {
    String path = SegmentUtils.getMasterZkPath(clusterName);
    JSONObject ret = new JSONObject();
    
    List<String> children = zkManager.getZkClient().getChildren(path);
  for (String controllerName : children) {
    byte[] data = zkManager.getZkClient().readData(path + "/" + controllerName);
    if (data != null) {
      ret.put(controllerName, MasterInfo.fromBytes(data).toJson());
    }
  }
    return ret;
  }

  public int movePartition(String partition, String segment, String moveParam) {
    int newPartition = Integer.parseInt(moveParam);
    if (newPartition > maxPartition) {
      throw new IllegalStateException("The new partition is bigger than max partition - " + maxPartition);
    }
    zkManager.moveSegment(segment, Integer.parseInt(partition), newPartition);
    return newPartition;
  }

  public void deleteSegment(String partition, String segment) {
    if (partition == null) {
      throw new IllegalStateException("The partition is not specified");
    }
    if (segment == null) {
      throw new IllegalStateException("The segment is not specified");
    }
    boolean result = zkManager.removeSegment(Integer.parseInt(partition), segment);
   if (!result) {
    throw new IllegalStateException("The segment doesn't exist");
   }
  }

  public void printSegments(HttpServletResponse resp, String partition, String segment) throws JSONException, IOException {
    if (partition == null) {
      JSONObject obj = new JSONObject();
      for (String partitionIt : zkManager.getPartitions()) {        
        obj.put(partitionIt, getPartitionJson(zkManager, partitionIt));
      }
      resp.getOutputStream().print(obj.toString(1));
      return;
    }
    if (segment == null) {
      JSONObject partitionJson = getPartitionJson(zkManager, partition);
      if (partitionJson != null) {
        resp.getOutputStream().print( partitionJson.toString(1));
      }
    } else {
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(segment);
      if (segmentInfo != null) {
        resp.getOutputStream().print( segmentInfo.toJson().toString(1));
      }
    }
  }

  public static JSONObject getPartitionJson(ZkManager zkManager, String partitionIt) throws JSONException {
    JSONObject partitionJson = new JSONObject();
    for (String segmentId : zkManager.getSegmentsForPartition(partitionIt)) {
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(segmentId);
      if (segmentInfo != null) {
        partitionJson.put(segmentId, segmentInfo.toJson());
      }
    }
    return partitionJson;
  }
  
  public String getPartition(String pathInfo) {
    if (pathInfo != null && pathInfo.length() > 1 && pathInfo.startsWith("/")) {
      int endIndex = pathInfo.indexOf("/", 1);
      if (endIndex < 0) {
        endIndex = pathInfo.length();
      }
      
      String partition = pathInfo.substring(1, endIndex);
      if (partition.length() > 0) {
        return partition;
      }
    }
    return null;
  }
  public String getSegmentId(String pathInfo) {
    if (pathInfo != null && pathInfo.length() > 1 && pathInfo.startsWith("/")) {
      int startIndex = pathInfo.indexOf("/", 1);
      if (startIndex < 0 || pathInfo.length() <= startIndex + 1) {
        return null;
      }
      int endIndex = pathInfo.indexOf("/", startIndex + 1);
      if (endIndex < 0) {
        endIndex = pathInfo.length();
      }
      String segmentId = pathInfo.substring(startIndex + 1, endIndex);
      if (segmentId.length() > 0) {
        return segmentId;
      }
    }
    return null;
  }
 

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doGet(req, resp);
  }

 
  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      super.service(req, resp);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      try {
        ex.printStackTrace(new PrintWriter(resp.getOutputStream()));
        resp.getOutputStream().flush();
        resp.getOutputStream().close();
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }
 

}
