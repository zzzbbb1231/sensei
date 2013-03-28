package com.senseidb.ba.gazelle.custom;

public class GazelleCustomIndexRegistry {
  public static GazelleCustomIndex get(String name) {
    try {
      return (GazelleCustomIndex) Class.forName(name).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
