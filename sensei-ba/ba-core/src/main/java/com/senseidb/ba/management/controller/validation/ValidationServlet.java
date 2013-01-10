package com.senseidb.ba.management.controller.validation;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.management.ZookeeperTracker;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.res.SenseiResult;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.NetUtil;

public class ValidationServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(ValidationServlet.class);  
 
  private String clusterName;
  private ZkManager zkManager;

  private String brokerUrl;

  private SenseiServiceProxy senseiServiceProxy;
  
  @Override
  public void init(ServletConfig config) throws ServletException {   
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    clusterName = config.getInitParameter("clusterName");
    zkManager = new ZkManager(zkUrl, clusterName);
    brokerUrl = config.getInitParameter("brokerUrl");
    if (!brokerUrl.endsWith("/")) {
      brokerUrl = brokerUrl + "/";
    }
    senseiServiceProxy = new SenseiServiceProxy(brokerUrl + "sensei");
    Assert.notNull(brokerUrl, "brokerUrl parameter should be present");
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      List<String> segments = new ArrayList<String>(100);
      Set<String> presentSegments = new HashSet<String>();
      for (String partition : zkManager.getPartitions()) {
        segments.addAll(zkManager.getSegmentsForPartition(partition));
      }
      logger.info("There are " + segments.size() + " segment in total");
      String bql = "{\"bql\":\"select com.senseidb.ba.management.controller.validation.GetActiveSegmentsMapReduceJob('')\"}";
      String result = senseiServiceProxy.sendPostRaw(brokerUrl + "sensei", new JSONObject(bql).toString());
      JSONObject jsonResp = new JSONObject(result);
      
      JSONObject mapReduceResult = jsonResp.optJSONObject("mapReduceResult");
      if (mapReduceResult != null) {
      JSONArray presentSegmentsJson = mapReduceResult.getJSONArray("result");
      for (int i = 0; i < presentSegmentsJson.length(); i++) {
        presentSegments.add(presentSegmentsJson.getString(i));
      }
      }
      List<String> missingSegments = new ArrayList<String>(segments);
      missingSegments.removeAll(presentSegments);
      logger.info("There are " + missingSegments.size() + "segments missing");
      final Map<String, SegmentInfo> segmentInfos = new HashMap<String, SegmentInfo>();
      for (String missingSegment : segments) {
        SegmentInfo segmentInfo = zkManager.getSegmentInfo(missingSegment);
        if (segmentInfo.getConfig().containsKey(SegmentMetadata.SEGMENT_END_TIME) && segmentInfo.getConfig().containsKey(SegmentMetadata.SEGMENT_START_TIME)) {
          segmentInfos.put(missingSegment, segmentInfo);
        }
      }
      Collections.sort(segments, new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
          return (int)(getStartTime(segmentInfos, o1) - getStartTime(segmentInfos, o2));
        }
      });
      List<String> missingPeriods = new ArrayList<String>();
      List<String> overlappingPeriods = new ArrayList<String>();
      List<String> overlappingSegments = new ArrayList<String>();
      List<String> duplicateSegments = new ArrayList<String>();
      for (int i = 0; i < segments.size() - 1; i++) {
        String segmentId1 = segments.get(i);
        String segmentId2 = segments.get(i + 1);
        long startTime1 = getStartTime(segmentInfos, segmentId1);
        long endTimeTime1 = getEndTime(segmentInfos, segmentId1);
        long startTime2 = getStartTime(segmentInfos, segmentId2);
        long endTimeTime2 = getEndTime(segmentInfos, segmentId2);
        if (startTime1 == startTime2 && endTimeTime1 == endTimeTime2) {
          duplicateSegments.add(segmentId1 + "," + segmentId2);
        } else  if (endTimeTime1 + 1 < startTime2) {
          missingPeriods.add((endTimeTime1 + 1) + "," + (startTime2 - 1));
        } else  if (startTime2  <= endTimeTime1) {
          overlappingPeriods.add((startTime2 - 1) + "," + (endTimeTime1 + 1));
          overlappingSegments.add(segmentId1 + "," + segmentId2);
        }
      }
    JSONObject ret = new JSONObject();
    ret.put("missingPeriods", new JSONArray(missingPeriods));
    ret.put("failedSegments", new JSONArray(missingSegments));
    ret.put("overlappingPeriods", new JSONArray(overlappingPeriods));
    ret.put("overlappingSegments", new JSONArray(overlappingSegments));
    ret.put("duplicateSegments", new JSONArray(duplicateSegments));
    resp.getOutputStream().println(ret.toString(1));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      resp.getOutputStream().flush();
    }
    
  }
  public long getStartTime(final Map<String, SegmentInfo> segmentInfos, String segmentId) {
    return Long.parseLong(segmentInfos.get(segmentId).getConfig().get(SegmentMetadata.SEGMENT_START_TIME));
  }
  public long getEndTime(final Map<String, SegmentInfo> segmentInfos, String segmentId) {
    return Long.parseLong(segmentInfos.get(segmentId).getConfig().get(SegmentMetadata.SEGMENT_END_TIME));
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

