package com.senseidb.ba.gazelle;

public enum SegmentTimeType {
  millisSinceEpoch, secondsSinceEpoch, minutesSinceEpoch, hoursSinceEpoch, daysSinceEpoch, monthsSinceEpoch, yearsSinceEpoch; 

  public static SegmentTimeType valueOfStr(String value) throws IllegalAccessException {
    for (SegmentTimeType timeType : SegmentTimeType.values()) {
      if (timeType.toString().toLowerCase().equals(value.toLowerCase())) {
        return timeType;
      }
    }
    if ("daySinceEpoch".equalsIgnoreCase(value)) {
      return daysSinceEpoch;
    }
    throw new IllegalAccessException("Unsupported time type");
  }
  public long toMillis(long time) {
    switch (this) {
     case millisSinceEpoch : return time; 
     case secondsSinceEpoch : return time * 1000;
     case minutesSinceEpoch : return time * 1000 * 60;
     case hoursSinceEpoch : return time * 1000 * 60 * 60;
     case daysSinceEpoch : return time * 1000 * 60 * 60* 24;
     case monthsSinceEpoch : return time * 1000* 60 * 60* 24 * 30;
     case yearsSinceEpoch : return time * 1000* 60 * 60* 24 * 30 * 365;
    }
    throw new UnsupportedOperationException();
  } 
  
}