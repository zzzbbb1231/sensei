package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Arrays;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class CompositeMetricsIntegrationTest  extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
  private static File compressedSegment;
  private static GazelleIndexSegmentImpl indexSegmentImpl;
  private static String httpUploadDirectory;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index"));
    SingleNodeStarter.rmrf(indexDir);
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    //System.setProperty("com.linkedin.norbert.disableJMX", "true");
    indexDir = new File("testIndex");
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/partitions/testCluster2");    
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(CompositeMetricsIntegrationTest.class.getClassLoader().getResource("ba-conf").toURI());
     httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    //createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    File compressedFile = TestUtil.flushIndexSegmentForCompositeMetricsAndCompress(indexDir, "segmentWithMetrics");
    FileInputStream inputStream = new FileInputStream(compressedFile);
    String port =  "8088";
    FileUploadUtils.sendFile("localhost", port, "segmentWithMetrics", inputStream, compressedFile.length());
    IOUtils.closeQuietly(inputStream);
    SingleNodeStarter.waitTillServerStarts(1000);
  }


  @Test
  public void test1SelectAll() throws Exception {
  
    String req = "{\"bql\":\"select * where country_code = 'ad' limit 0\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 196, resp.getInt("numhits"));
   
  }
  @Test
  public void test2SelectAvg() throws Exception {
  
    String req = "{\"bql\":\"select avg(metric_46_n_mu) \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 1000, resp.getInt("numhits"));
    assertEquals("15405.88200", resp.getJSONObject("mapReduceResult").getString("avg"));
   
  }
  @Test
  public void test3SelectSumGroupBy() throws Exception {
  
    String req = "{\"bql\":\"select sum(metric_46_n_mu) group by country_code limit 0\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 1000, resp.getInt("numhits"));
    assertEquals(12418343, resp.getJSONObject("facets").getJSONArray("_sumGroupBy").getJSONObject(0).getInt("count"));
   
  }
}
