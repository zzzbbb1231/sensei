package com.linkedin.gazelle.utils;

import org.apache.avro.util.Utf8;

/**
 * @author dpatel
 */

public enum GazelleColumnType {
  INT, LONG, STRING, FLOAT;

  public static GazelleColumnType getTypeFromObject(Object obj) {
    if (obj instanceof Long) {
      return LONG;
    }
    if (obj instanceof Integer) {
      return INT;
    }
    if (obj instanceof String) {
      return STRING;
    }
    if (obj instanceof Float || obj instanceof Double) {
      return FLOAT;
    }
    throw new UnsupportedOperationException(obj.toString());
  }

  public static GazelleColumnType getType(String klassName) {
    if (klassName.toUpperCase().equals("DOUBLE") || klassName.toUpperCase().equals("FLOAT")) {
      return FLOAT;
    } else if (klassName.toUpperCase().equals("INT")) {
      return INT;
    } else if (klassName.toUpperCase().equals("LONG")) {
      return LONG;
    } else if (klassName.toUpperCase().equals("STRING")) {
      return STRING;
    }
    throw new UnsupportedOperationException(klassName);
  }
}
