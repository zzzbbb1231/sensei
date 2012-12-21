package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.gazelle.SegmentAggregationLevel;
import com.senseidb.ba.gazelle.SegmentTimeType;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.ba.util.Wait;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.SingleNodeStarter;

public class RestSegmentManagementTest  extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
  private static File compressedSegment;
  private static GazelleIndexSegmentImpl indexSegmentImpl;
  private static String httpUploadDirectory;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    indexDir = new File("testIndex");
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/testCluster2");    
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(RestSegmentManagementTest.class.getClassLoader().getResource("ba-conf").toURI());
     httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    //createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    indexSegmentImpl = TestUtil.createIndexSegment();
    for (int i = 0; i < 2; i++) {
      if (i == 1) {
        indexSegmentImpl.getSegmentMetadata().setEndTime("0");
        indexSegmentImpl.getSegmentMetadata().setTimeType(SegmentTimeType.secondsSinceEpoch);
      } else {
        indexSegmentImpl.getSegmentMetadata().setEndTime("" + System.currentTimeMillis());
        indexSegmentImpl.getSegmentMetadata().setTimeType(SegmentTimeType.secondsSinceEpoch);
      }
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      FileInputStream inputStream = new FileInputStream(compressedFile);
      FileUploadUtils.sendFile("localhost", "8088", "segment" + i, inputStream, compressedFile.length());
      IOUtils.closeQuietly(inputStream);
    }
    SingleNodeStarter.waitTillServerStarts(20000);

  }

  

  @Test
  public void test1ManagementBackend() throws Exception {
    String stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/segments/");
    JSONObject json = new JSONObject(stringResponse);
    String[] names = JSONObject.getNames(json);
    Arrays.sort(names);
    assertEquals("[0, 1]", Arrays.toString(names));
    names = JSONObject.getNames(json.getJSONObject("0"));
    Arrays.sort(names);
    assertEquals("[segment1]", Arrays.toString(names));
    stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/segments/0/segment1?move=1");
    assertTrue(stringResponse, stringResponse.startsWith("Succesfully moved segment - segment1 to the new partition - 1"));
    stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/segments/");
     json = new JSONObject(stringResponse);
     names = JSONObject.getNames(json.getJSONObject("0"));     
     assertNull(names);
     names = JSONObject.getNames(json.getJSONObject("1")); 
     Arrays.sort(names);
     assertEquals("[segment0, segment1]", Arrays.toString(names));
     FileUploadUtils.getStringResponse("http://localhost:8088/segments/1/segment0?delete");
     stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/segments/");
     json = new JSONObject(stringResponse);
     names = JSONObject.getNames(json.getJSONObject("1")); 
     assertEquals("[segment1]", Arrays.toString(names));
  }
  @Test
  public void test3DeleteSegments() throws Exception {
    SenseiPluginRegistry senseiPluginRegistry = null;
    try {File ConfDir1 = new File(RestSegmentManagementTest.class.getClassLoader().getResource("controller-conf").toURI());
    File senseiConfFile = new File(ConfDir1, SenseiServerBuilder.SENSEI_PROPERTIES);
    Configuration senseiConf = new PropertiesConfiguration();
    ((PropertiesConfiguration) senseiConf).setDelimiterParsingDisabled(true);
    ((PropertiesConfiguration) senseiConf).load(senseiConfFile);
     senseiPluginRegistry = SenseiPluginRegistry.build(senseiConf);
    senseiPluginRegistry.start();
    new Wait(5000){
      public boolean until() {
        System.out.println(zkManager.getZkClient().getChildren(SegmentUtils.getSegmentInfoPath(zkManager.getClusterName())));
        return  zkManager.getZkClient().getChildren(SegmentUtils.getSegmentInfoPath(zkManager.getClusterName())).size() == 1;};
    }; 
    } finally {
      senseiPluginRegistry.stop();
    }
  }
  @Test
  public void test4NasSupport() throws Exception {
    tearDown();
    SenseiPluginRegistry senseiPluginRegistry = null;
    try {File ConfDir1 = new File(RestSegmentManagementTest.class.getClassLoader().getResource("controller-conf").toURI());
    File senseiConfFile = new File(ConfDir1, SenseiServerBuilder.SENSEI_PROPERTIES);
    Configuration senseiConf = new PropertiesConfiguration();
    ((PropertiesConfiguration) senseiConf).setDelimiterParsingDisabled(true);
    ((PropertiesConfiguration) senseiConf).load(senseiConfFile);
     senseiPluginRegistry = SenseiPluginRegistry.build(senseiConf);
     senseiPluginRegistry.start();
     indexSegmentImpl = TestUtil.createIndexSegment();
     File compressedFile = TestUtil.createCompressedSegment("nasSegment", indexSegmentImpl, indexDir);
     FileInputStream inputStream = new FileInputStream(compressedFile);
     FileUploadUtils.sendFile("localhost", "7088", "nasSegment", inputStream, compressedFile.length());
     Thread.sleep(1000L);
     SegmentInfo segmentInfo = zkManager.getSegmentInfo("nasSegment");
     assertEquals("[/tmp/fileUpload/testCluster2/nasSegment]", segmentInfo.getPathUrls().toString());
    } finally {
      try {
      senseiPluginRegistry.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  @Test
  public void test2AddSegmentAndModifyItAfter() throws Exception {
    tearDown();
    HashMap<String,String> config = new HashMap<String, String>();
    config.put("key1", "prop1");
    SegmentInfo info = new SegmentInfo("segment", Arrays.asList("url1"), config);
    zkManager.registerSegment(0, "segm", "path1", System.currentTimeMillis());
    zkManager.registerSegment(0, "segm", "path2", System.currentTimeMillis());
    SegmentInfo segmentInfo = zkManager.getSegmentInfo("segm");
    assertEquals(2, new HashSet<String>(segmentInfo.getPathUrls()).size());
  }

}
