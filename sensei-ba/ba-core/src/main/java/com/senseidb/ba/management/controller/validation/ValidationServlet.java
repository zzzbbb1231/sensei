package com.senseidb.ba.management.controller.validation;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TimerService;
import com.senseidb.search.client.SenseiServiceProxy;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

public class ValidationServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(ValidationServlet.class);  
  private String clusterName;
  private ZkManager zkManager;

  private String brokerUrl;

  private SenseiServiceProxy senseiServiceProxy;
  private static final Counter missingPeriodsCounter = Metrics.newCounter(ValidationServlet.class, "missingPeriods");;
  private static final Counter failedSegmentsCounter = Metrics.newCounter(ValidationServlet.class, "failedSegments");;
  private static final Counter overlappingPeriodsCounter = Metrics.newCounter(ValidationServlet.class, "overlappingPeriods");
  private static final Counter duplicateSegmentsCounter = Metrics.newCounter(ValidationServlet.class, "duplicateSegments");
  private static final Counter currentDelayInDaysCounter = Metrics.newCounter(ValidationServlet.class, "currentDelayInDays");
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
    TimerService.timer.scheduleAtFixedRate(new TimerTask() {
      
      @Override
      public void run() {
        logger.info("Updating the validation metrics");
        try {
          getValidationDataAsJson();
        } catch (JSONException e) {
         logger.error("An exception in the healthCheck thread", e);
        }
      }
    }, 10 * 60 * 1000, 60 * 60 * 1000);
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      JSONObject ret = getValidationDataAsJson();
    resp.getOutputStream().println(ret.toString(1));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      resp.getOutputStream().flush();
    }
    
  }

  /**
   * This would always update counters
   * @throws JSONException
   */
  public JSONObject getValidationDataAsJson() throws JSONException {
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
    Iterator<String> segmentIterator = segments.iterator();
    //Filter out segment that doesn't have start and endtime
    while(segmentIterator.hasNext()) {
      String segment = segmentIterator.next();        
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(segment);
      if (segmentInfo.getConfig().containsKey(SegmentMetadata.SEGMENT_END_TIME) && segmentInfo.getConfig().containsKey(SegmentMetadata.SEGMENT_START_TIME)) {
        segmentInfos.put(segment, segmentInfo);
      } else {
        segmentIterator.remove();
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
    missingPeriodsCounter.clear();
    missingPeriodsCounter.inc(missingPeriods.size());
  ret.put("failedSegments", new JSONArray(missingSegments));
  failedSegmentsCounter.clear();
  failedSegmentsCounter.inc(missingSegments.size());
  ret.put("overlappingPeriods", new JSONArray(overlappingPeriods));
  overlappingPeriodsCounter.clear();
  overlappingPeriodsCounter.inc(overlappingPeriods.size());
  ret.put("overlappingSegments", new JSONArray(overlappingSegments));

  ret.put("duplicateSegments", new JSONArray(duplicateSegments));
  duplicateSegmentsCounter.clear();
  duplicateSegmentsCounter.inc(duplicateSegments.size());
 
  if (segments.size() > 0) {
    ret.put("minTime", getStartTime(segmentInfos, segments.get(0)));
    long endTime = getEndTime(segmentInfos, segments.get(segments.size() - 1));      
    ret.put("maxTime", endTime);
    long currentDelay = getCurrentDaysSinceEpoch() - endTime;
    ret.put("currentDelayInDays", currentDelay);
    currentDelayInDaysCounter.clear();
    currentDelayInDaysCounter.inc(currentDelay);
  }
    return ret;
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
 public static void main(String[] args) {
   Calendar c = new GregorianCalendar();
   c.setTime(new Date(0));
   System.err.println(c.get(Calendar.DAY_OF_YEAR));
}
public static int getCurrentDaysSinceEpoch() {
  MutableDateTime epoch = new MutableDateTime();
  epoch.setDate(0); //Set to Epoch time
  DateTime now = new DateTime();

  Days days = Days.daysBetween(epoch, now);
  return days.getDays();
}
}

