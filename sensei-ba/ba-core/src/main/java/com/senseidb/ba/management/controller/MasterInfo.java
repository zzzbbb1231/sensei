package com.senseidb.ba.management.controller;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.json.JSONObject;

public class MasterInfo {
  private String date;
  private String nodeId;
  public MasterInfo() {
  }
  
  public MasterInfo(String date, String nodeId) {
    super();
    this.date = date;
    this.nodeId = nodeId;
  }
  public String getDate() {
    return date;
  }
  public String getNodeId() {
    return nodeId;
  }
  public void setDate(String date) {
    this.date = date;
  }
  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }
  public byte[] toBytes() throws IOException {
    Properties properties = new Properties();
    
      properties.put("date", date);
      properties.put("nodeId", nodeId);
      
    ByteArrayOutputStream metadataBytes = new ByteArrayOutputStream();
    properties.store(metadataBytes, "");
    return metadataBytes.toByteArray();
  }
  public static MasterInfo fromBytes(byte[] bytes) {
    try {
    MasterInfo masterInfo = new MasterInfo();
    Properties properties = new Properties();
      properties.load(new ByteArrayInputStream(bytes));
    masterInfo.date = (String) properties.get("date");
    masterInfo.nodeId = (String) properties.get("nodeId");
    return masterInfo;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  public JSONObject toJson() {
    try {
    JSONObject ret = new JSONObject();
    ret.put("date", date);
    ret.put("nodeId", nodeId);
    return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
