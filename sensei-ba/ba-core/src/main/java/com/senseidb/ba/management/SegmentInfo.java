package com.senseidb.ba.management;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SegmentInfo {
  private String segmentId;
  private String pathUrl;
  private SegmentType type;
  private long timeCreated;
  private long timeToLive;
  public SegmentInfo() {
    // TODO Auto-generated constructor stub
  }
  
  public SegmentInfo(String segmentId, String pathUrl, SegmentType type, long timeCreated, long timeToLive) {
    super();
    this.segmentId = segmentId;
    this.pathUrl = pathUrl;
    this.type = type;
    this.timeCreated = timeCreated;
    this.timeToLive = timeToLive;
  }

  public byte[] toByteArray() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1000);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeUTF(segmentId);
   
    dataOutputStream.writeUTF(pathUrl);
    dataOutputStream.writeUTF(type.toString());
    dataOutputStream.writeLong(timeCreated);
    dataOutputStream.writeLong(timeToLive);
    dataOutputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return byteArrayOutputStream.toByteArray();
  }
  public static SegmentInfo fromBytes(byte[] bytes) {
    SegmentInfo segmentInfo = new SegmentInfo();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    try {
      segmentInfo.segmentId = dataInputStream.readUTF();
      segmentInfo.pathUrl = dataInputStream.readUTF();
      segmentInfo.type = SegmentType.valueOf(dataInputStream.readUTF());
      segmentInfo.timeCreated = dataInputStream.readLong();
      segmentInfo.timeToLive = dataInputStream.readLong();
      dataInputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return segmentInfo;
  }
  public String getSegmentId() {
    return segmentId;
  }
  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }
  public String getPathUrl() {
    return pathUrl;
  }
  public String[] getPathUrls() {
    return pathUrl.split(",");
  }
  public void setPathUrl(String pathUrl) {
    this.pathUrl = pathUrl;
  }
  public SegmentType getType() {
    return type;
  }
  public void setType(SegmentType type) {
    this.type = type;
  }
  public long getTimeCreated() {
    return timeCreated;
  }
  public void setTimeCreated(long timeCreated) {
    this.timeCreated = timeCreated;
  }
  public long getTimeToLive() {
    return timeToLive;
  }
  public void setTimeToLive(long timeToLive) {
    this.timeToLive = timeToLive;
  }
  
}
