package com.senseidb.ba;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.senseidb.plugin.SenseiPluginRegistry;

import junit.framework.TestCase;

public class ControllerTest extends TestCase {
  public void test1() {
    SenseiPluginRegistry senseiPluginRegistry = null;
    try {
    Map<String, String> map = new HashMap<String, String>();
    map.put("deletionController.class", "com.senseidb.ba.management.controller.DeletionController");
    map.put("deletionController.retention", "30");
    map.put("deletionController.timeUnit", "SECONDS");
    map.put("deletionController.frequency", "1000");
    map.put("deletionController.clusterName", "testCluster2");
    map.put("deletionController.zkUrl", "localhost:2181");
    map.put("deletionController.nodeId", "1");
    map.put("deletionController.maxPartitionId", "2");
    map.put("httpServer.class", "com.senseidb.ba.file.http.JettyServerHolder");
    map.put("httpServer.port", "7087");
    map.put("httpServer.directory", "/tmp/fileUpload");
    map.put("httpServer.nasBasePath", "/tmp/fileUpload");
    map.put("httpServer.clusterName", "testCluster2");
    map.put("httpServer.zkUrl", "localhost:2181");
    map.put("httpServer.nodeId", "1");
    map.put("httpServer.maxPartitionId", "2");
    MapConfiguration senseiConf = new MapConfiguration(map);
    senseiConf.setDelimiterParsingDisabled(true);
    senseiPluginRegistry = SenseiPluginRegistry.build(senseiConf);
    senseiPluginRegistry.start();
    } finally {
      senseiPluginRegistry.stop();
    }
  }
}
