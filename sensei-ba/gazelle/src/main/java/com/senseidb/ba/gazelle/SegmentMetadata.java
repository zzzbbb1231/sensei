package com.senseidb.ba.gazelle;

import java.util.HashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

public class SegmentMetadata {
  public static final String SEGMENT_CLUSTER_NAME = "segment.cluster.name";
  public static final String SEGMENT_END_TIME = "segment.endTime";
  public static final String SEGMENT_START_TIME = "segment.startTime";
  public static final String SEGMENT_CRC = "segment.crc";
  private String clusterName;
  private SegmentTimeType timeType;
  private SegmentAggregationLevel aggregationLevel;
  private String startTime;
  private String endTime;
  private String crc = null;
  private HashMap<String, String> addionalEntries = new HashMap<String, String>();
  private static String[] defaultList = {SEGMENT_CLUSTER_NAME,"segment.time.Type","segment.aggregation",SEGMENT_START_TIME,SEGMENT_END_TIME, SEGMENT_CRC};

  public void addToConfig(Configuration configuration) {
    if (clusterName != null) {
      configuration.setProperty(SEGMENT_CLUSTER_NAME, clusterName);
    }
    
    if (timeType != null) {
      configuration.setProperty("segment.time.Type", timeType.toString());
    }
    
    if (aggregationLevel != null) {
      configuration.setProperty("segment.aggregation", aggregationLevel.toString());
    }
    
    if (startTime != null) {
      configuration.setProperty(SEGMENT_START_TIME, startTime);
    }
    
    if (endTime != null) {
      configuration.setProperty(SEGMENT_END_TIME, endTime);
    }
    if (crc != null) {
      configuration.setProperty(SEGMENT_CRC, crc);
    }
    for (String key : addionalEntries.keySet()) {
      configuration.setProperty(key, addionalEntries.get(key));
    }
  }

  public static boolean isFromDefaultList(String key) {
    for (String entry : defaultList) {
      if (key.equals(entry)) {
        return true;
      }
    }
    return false;
  }

  public void put(String key, String value)  {
    if (StringUtils.isEmpty(key) && StringUtils.isEmpty(value)) {
      throw new IllegalStateException("Cannot have an empty key or value");
    }
    if (!StringUtils.startsWith(key, "segment.")) {
      throw new IllegalStateException("keys should start with segment.");
    }
    addionalEntries.put(key, value);
  }

  public String get(String customEntryKey) {
    return addionalEntries.get(customEntryKey);
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public SegmentTimeType getTimeType() {
    return timeType;
  }

  public void setTimeType(String timeType) throws IllegalAccessException {
    this.timeType = SegmentTimeType.valueOfStr(timeType);
  }

  public void setTimeType(SegmentTimeType timeType) {
    this.timeType = timeType;
  }

  public SegmentAggregationLevel getAggregationLevel() {
    return aggregationLevel;
  }

  public void setAggregationLevel(String aggregationLevel) {
    this.aggregationLevel = SegmentAggregationLevel.valueOfStr(aggregationLevel);
  }

  public void setAggregationLevel(SegmentAggregationLevel aggregationLevel) {
    this.aggregationLevel = aggregationLevel;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public String getCrc() {
    return crc;
  }

  public void setCrc(String crc) {
    this.crc = crc;
  }
  
}
