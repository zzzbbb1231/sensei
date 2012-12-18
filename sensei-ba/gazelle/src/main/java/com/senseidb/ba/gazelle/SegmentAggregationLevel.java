package com.senseidb.ba.gazelle;

public enum SegmentAggregationLevel {
  SECONDS, MINUTES, HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY;

  public static SegmentAggregationLevel valueOfStr(String value) {
    String val = value.toUpperCase();
    return valueOf(val);
  }
}
