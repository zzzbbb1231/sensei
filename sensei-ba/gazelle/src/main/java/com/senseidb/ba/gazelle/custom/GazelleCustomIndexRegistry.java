package com.senseidb.ba.gazelle.custom;

public class GazelleCustomIndexRegistry {
  public static GazelleCustomIndex get(String name) {
    try {
      if (name.equalsIgnoreCase("com.senseidb.ba.gazelle.index.custom.CompositeMetricCustomIndex")) {
        return new CompositeMetricCustomIndex();
      }
      return (GazelleCustomIndex) Class.forName(name).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
