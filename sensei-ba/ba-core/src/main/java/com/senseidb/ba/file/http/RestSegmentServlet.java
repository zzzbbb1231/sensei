package com.senseidb.ba.file.http;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.util.NetUtil;

public class RestSegmentServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(RestSegmentServlet.class);  
 
  private String clusterName;
  private ZkManager zkManager;
  private int maxPartition;
  
  @Override
  public void init(ServletConfig config) throws ServletException {   
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    clusterName = config.getInitParameter("clusterName");
    Assert.notNull(clusterName, "clusterName parameter should be present");
    zkManager = new ZkManager(zkUrl, clusterName);
    String maxPartitionId = config.getInitParameter("maxPartitionId");
    Assert.notNull(maxPartition, "maxPartition parameter should be present");
    maxPartition = Integer.parseInt(maxPartitionId);
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
    String pathInfo = req.getPathInfo();
    String  partition = getPartition(pathInfo);
    String  segment = getSegmentId(pathInfo);
    String deleteParam = req.getParameter("delete");
    String moveParam = req.getParameter("move");
    
    if (deleteParam != null ) {
      deleteSegment(partition, segment);
      resp.getOutputStream().println("Succesfully deleted segment - " + segment);
    } else if (moveParam != null) {
      int newPartition = movePartition(partition, segment, moveParam);
      resp.getOutputStream().println("Succesfully moved segment - " + segment + " to the new partition - " + newPartition);
    } else { 
      printSegments(resp, partition, segment);
    }
    
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      resp.getOutputStream().flush();
    }
    
  }

  public int movePartition(String partition, String segment, String moveParam) {
    int newPartition = Integer.parseInt(moveParam);
    if (newPartition > maxPartition) {
      throw new IllegalStateException("The new partition is bigger than max partition - " + maxPartition);
    }
    SegmentInfo segmentId = zkManager.getSegmentInfo(partition, segment);
    if (segmentId == null) {
      throw new IllegalStateException("The segment doesn't exist - " + segment);
    }
    deleteSegment(partition, segment);
    zkManager.registerSegment(newPartition, segment, segmentId.getPathUrl(), segmentId.getType(), segmentId.getTimeCreated(), segmentId.getTimeToLive());
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
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(partition, segment);
      if (segmentInfo != null) {
        resp.getOutputStream().print( segmentInfo.toJson().toString(1));
      }
    }
  }

  public JSONObject getPartitionJson(ZkManager zkManager, String partitionIt) throws JSONException {
    JSONObject partitionJson = new JSONObject();
    for (String segmentId : zkManager.getSegments(partitionIt)) {
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(partitionIt, segmentId);
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

