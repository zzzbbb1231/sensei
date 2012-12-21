package com.senseidb.ba.file.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.ba.management.ZkManager;

public class AllClustersRestSegmentServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(RestSegmentServlet.class);  
 
  private int maxPartition;

  private ZkClient zkClient;
  
  @Override
  public void init(ServletConfig config) throws ServletException {   
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");    
    zkClient = new ZkClient(zkUrl);
    zkClient.setZkSerializer(new BytesPushThroughSerializer());
    String maxPartitionId = config.getInitParameter("maxPartitionId");
    Assert.notNull(maxPartition, "maxPartition parameter should be present");
    maxPartition = Integer.parseInt(maxPartitionId);
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
    String pathInfo = req.getPathInfo();
    String  clusterName = null;
    String  partition = null;
    String  segment = null;
    List<String> tokenized = tokenizePathInfo(pathInfo);
    int index = 0;
    if (index < tokenized.size()) {
      String firstPart = tokenized.get(index);
      if (!StringUtils.isNumeric(firstPart)) {
        clusterName = firstPart;
        index++;
      }
    }
    if (index < tokenized.size()) {
       partition = tokenized.get(index);
        index++;
    }
    if (index < tokenized.size()) {
      segment = tokenized.get(index);
       index++;
   }
    String deleteParam = req.getParameter("delete");
    String moveParam = req.getParameter("move");
    
    if (deleteParam != null ) {
      deleteSegment(clusterName, partition, segment);
      resp.getOutputStream().println("Succesfully deleted segment - " + segment);
    } else if (moveParam != null) {
      int newPartition = movePartition(clusterName, partition, segment, moveParam);
      resp.getOutputStream().println("Succesfully moved segment - " + segment + " to the new partition - " + newPartition);
    } else { 
      printSegments(resp, clusterName, partition, segment);
    }
    
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      resp.getOutputStream().flush();
    }
    
  }

  public int movePartition(String clusterName, String partition, String segment, String moveParam) {
    int newPartition = Integer.parseInt(moveParam);
    if (newPartition > maxPartition) {
      throw new IllegalStateException("The new partition is bigger than max partition - " + maxPartition);
    }
    SegmentUtils.moveSegment(zkClient, clusterName, segment, Integer.parseInt(partition), Integer.parseInt(moveParam));
    return newPartition;
  }

  public void deleteSegment(String clusterName, String partition, String segment) {
    if (partition == null) {
      throw new IllegalStateException("The partition is not specified");
    }
    if (segment == null) {
      throw new IllegalStateException("The segment is not specified");
    }
    boolean result = SegmentUtils.removeFromActiveSegments(zkClient, clusterName, Integer.parseInt(partition), segment);
   if (!result) {
    throw new IllegalStateException("The segment doesn't exist");
   }
  }
 public static List<String> tokenizePathInfo(String pathInfo) {
   if (pathInfo.contains("?")) {
     pathInfo = pathInfo.substring(0, pathInfo.indexOf("?"));
   }
   List<String> ret = new ArrayList<String>();
   for (String part : pathInfo.split("/")) {
     part = part.trim();
     if (part.length() > 0) {
       ret.add(part);
     }
   }
   return ret;
 }
  public void printSegments(HttpServletResponse resp, String clusterName, String partition, String segment) throws JSONException, IOException {
    if (clusterName == null) {
      JSONObject obj = new JSONObject();
      for (String clusterNameIt : SegmentUtils.getClusterNames(zkClient)) {
        obj.put(clusterNameIt, getClusterJson(clusterNameIt));
      }
      resp.getOutputStream().print(obj.toString(1));
      return;
    }
    if (partition == null) {
      JSONObject obj = getClusterJson(clusterName);
      resp.getOutputStream().print(obj.toString(1));
      return;
    }
    ZkManager zkManager = new ZkManager(zkClient, clusterName);
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

  public JSONObject getClusterJson(String clusterName) throws JSONException {
    try {
    ZkManager zkManager = new ZkManager(zkClient, clusterName);
    JSONObject obj = new JSONObject();
    for (String partitionIt : zkManager.getPartitions()) {        
      obj.put(partitionIt, getPartitionJson(zkManager, partitionIt));
    }
    return obj;
    } catch (Exception ex) {
      logger.error(clusterName + " : " + ex.getMessage(), ex);
      return new JSONObject().put("error", ex.getMessage());
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