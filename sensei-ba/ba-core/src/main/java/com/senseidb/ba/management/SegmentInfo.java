package com.senseidb.ba.management;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import  static com.senseidb.ba.management.SegmentUtils.*;
public class SegmentInfo {
  private static Logger logger = Logger.getLogger(SegmentInfo.class);

  private String segmentId;
  private List<String> pathUrls;
  private Map<String, String> config;

  public SegmentInfo(String segmentId, List<String> pathUrls, Map<String, String> config) {
    super();
    this.segmentId = segmentId;
    this.pathUrls = pathUrls;
    this.config = config;
    if (!config.containsKey("segmentId")) {
      config.put("segmentId", segmentId);
    }
  }
 
  public void saveInfoToZookeeper(ZkClient zkClient, String clusterName) {
    String segmentInfoPath = getSegmentInfoPath(clusterName,  segmentId);
    try {
      if (!zkClient.exists(segmentInfoPath)) {
        zkClient.createPersistent(segmentInfoPath, true);
      }
    } catch (Exception ex) {
      logger.error(ex);
    }
    try {
      String metadataPath = segmentInfoPath + "/metadata";
      if (zkClient.exists(metadataPath)) {
        byte[] data = zkClient.readData(metadataPath);
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(data));
        boolean modified = false;
        for (String key : config.keySet()) {
          if (!properties.contains(key)) {
            modified = true;
          }
          properties.put(key, config.get(key));
        }
        if (modified) {
          ByteArrayOutputStream metadataBytes = new ByteArrayOutputStream();
          properties.store(metadataBytes, "");
          zkClient.writeData(metadataPath, metadataBytes.toByteArray());
        }
      } else {
        Properties properties = new Properties();
        for (String key : config.keySet()) {
          properties.put(key, config.get(key));
        }
        ByteArrayOutputStream metadataBytes = new ByteArrayOutputStream();
        properties.store(metadataBytes, "");
        zkClient.createPersistent(metadataPath, metadataBytes.toByteArray());
      }
    } catch (Exception ex) {
      logger.error(ex);
    }

    String urlsPath = segmentInfoPath + "/urls";
    try {
      if (!zkClient.exists(urlsPath)) {
        zkClient.createPersistent(urlsPath);
      }
    } catch (Exception ex) {
      logger.error(ex);
    }
    for (String url : pathUrls) {
      String zkTransformedUrl = transform(url);
      String urlPath = urlsPath + "/" + zkTransformedUrl;
      try {
        if (!zkClient.exists(urlPath)) {
          zkClient.createPersistent(urlPath, url.getBytes());
        }
      } catch (Exception ex) {
        logger.error(ex.getMessage(), ex);
      }
    }
    String readyPath = segmentInfoPath + "/readyFlag";
    try {
      if (!zkClient.exists(readyPath)) {
        zkClient.createPersistent(readyPath);
      }
    } catch (Exception ex) {
      logger.error(ex);
    }   
  }
  

  public static SegmentInfo retrieveFromZookeeper(ZkClient zkClient, String clusterName, String segmentId) {
    String zkPath = getSegmentInfoPath(clusterName, segmentId);
    if (!isSegmentInfoReady(zkClient, clusterName, segmentId)) {
      return null;
    }
    try {
      String metadataPath = zkPath + "/metadata";
      Map<String, String> metadata = new HashMap<String, String>();
      byte[] data = zkClient.readData(metadataPath);
      Properties properties = new Properties();
      properties.load(new ByteArrayInputStream(data));
      for (Object key : properties.keySet()) {
        Object value = properties.get(key);
        metadata.put(key.toString(), value != null ? value.toString() : null);
      }

      List<String> urls = new ArrayList<String>();
      String urlsPath = zkPath + "/urls";

      if (zkClient.exists(urlsPath)) {
        for (String child : zkClient.getChildren(urlsPath)) {
          byte[] dataBytes = zkClient.readData(urlsPath + "/" + child);
          urls.add(new String(dataBytes));
        }
      }
      return new SegmentInfo(metadata.get("segmentId"), urls, metadata);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String transform(String url) {
    int index = url.indexOf("//");
    if (index < 0) {
      return String.valueOf(url.hashCode());
    }
    int nextIndex = url.indexOf("/", index + 2);
    if (nextIndex < 0) {
      return url.substring(index + 2, url.length());
    }
    return url.substring(index + 2, nextIndex);
  }

  public String getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(String segmentId) {
    this.segmentId = segmentId;
  }

  public JSONObject toJson() {
    try {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("segmentId", segmentId);
      jsonObject.put("pathUrl", new JSONArray(pathUrls));
      for (String key : config.keySet()) {
        jsonObject.put(key, config.get(key));
      }
      return jsonObject;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public List<String> getPathUrls() {
    return pathUrls;
  }

  public Map<String, String> getConfig() {
    return config;
  }


}
