package com.senseidb.ba.gazelle;

public enum SegmentTimeType {
  secondsSinceEpoch, minutesSinceEpoch, hoursSinceEpoch, daysSinceEpoch, monthsSinceEpoch, yearsSinceEpoch; 

  public static SegmentTimeType valueOfStr(String value) throws IllegalAccessException {
    for (SegmentTimeType timeType : SegmentTimeType.values()) {
      if (timeType.toString().toLowerCase().equals(value.toLowerCase())) {
        return timeType;
      }
    }
    throw new IllegalAccessException("UnSupported time type");
  }
}