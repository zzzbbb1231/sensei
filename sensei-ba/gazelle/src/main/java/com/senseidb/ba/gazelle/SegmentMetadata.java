package com.senseidb.ba.gazelle;

import java.util.HashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

public class SegmentMetadata {
  private String clusterName;
  private SegmentTimeType timeType;
  private SegmentAggregationLevel aggregationLevel;
  private String startTime;
  private String endTime;
  private HashMap<String, String> addionalEntries = new HashMap<String, String>();
  private static String[] defaultList = {"segment.cluster.name","segment.time.Type","segment.aggregation","segment.startTime","segment.endTime"};

  public void addToConfig(Configuration configuration) {
    if (clusterName != null) {
      configuration.setProperty("segment.cluster.name", clusterName);
    }
    
    if (timeType != null) {
      configuration.setProperty("segment.time.Type", timeType.toString());
    }
    
    if (aggregationLevel != null) {
      configuration.setProperty("segment.aggregation", aggregationLevel.toString());
    }
    
    if (startTime != null) {
      configuration.setProperty("segment.startTime", startTime);
    }
    
    if (endTime != null) {
      configuration.setProperty("segment.endTime", endTime);
    }
    
    for (String key : addionalEntries.keySet()) {
      configuration.setProperty(key, addionalEntries.get(key));
    }
  }

  private boolean inNotNull(Object someVariable) {
    if (someVariable != null) {
      return true;
    }
    return false;
  }
  public static boolean isFromDefaultList(String key) {
    for (String entry : defaultList) {
      if (key.equals(entry)) {
        return true;
      }
    }
    return false;
  }

  public void put(String key, String value) throws IllegalAccessException {
    if (StringUtils.isEmpty(key) && StringUtils.isEmpty(value)) {
      throw new IllegalAccessException("Cannot have an empty key or value");
    }
    if (!StringUtils.startsWith(key, "segment.")) {
      throw new IllegalAccessException("keys should start with segment.");
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

}
