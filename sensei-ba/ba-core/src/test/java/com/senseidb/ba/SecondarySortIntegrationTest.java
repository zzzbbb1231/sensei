package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.List;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.format.GenericIndexCreator;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.Pair;
import com.senseidb.util.SingleNodeStarter;

public class SecondarySortIntegrationTest  extends Assert {

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
    zkClient.deleteRecursive("/sensei-ba/partitions/testCluster2");    
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(SecondarySortIntegrationTest.class.getClassLoader().getResource("ba-conf").toURI());
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
   indexSegmentImpl = GenericIndexCreator.create(indexSegmentImpl, new String[] {"shrd_advertiserId", "dim_memberIndustry"});
    assertTrue(indexSegmentImpl.getForwardIndex("dim_memberIndustry") instanceof SecondarySortedForwardIndex);
    for (int i = 0; i < 2; i++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      FileInputStream inputStream = new FileInputStream(compressedFile);
      FileUploadUtils.sendFile("localhost", "8088", "segment" + i, inputStream, compressedFile.length());
      IOUtils.closeQuietly(inputStream);
    }
    SingleNodeStarter.waitTillServerStarts(20000);

  }

  

  @Test
  public void test1FilterSecondarySortedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberIndustry\": {" + 
        "                \"values\": [\"102\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"," + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
        "}";
      
    JSONObject resp = null;
    
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
   
    assertEquals("numhits is wrong", 20, resp.getInt("numhits"));
  }
  @Test
  public void test2RangeFilterSecondarySortedColumn() throws Exception {
    
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 0,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberIndustry\": {" + 
        "                \"values\": [\"[101 TO 102]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"," + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
        "}";
      
    JSONObject resp = null;
    
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
     // Thread.sleep(10000000);
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 42, resp.getInt("numhits"));
  }
  
  @Test
  public void test3FacetCountSecondarySortedColumn() throws Exception {
    
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
            "    \"facets\": {\n" + 
        "        \"dim_memberIndustry\": {\n" + 
        "            \"max\": 10,\n" + 
        "            \"minCount\": 1,\n" + 
        "            \"expand\": false,\n" + 
        "            \"order\": \"hits\"\n" + 
        "        }\n" + 
        "    }" + 
        "}";
      
    JSONObject resp = null;
    
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 20000, resp.getInt("numhits"));
  }
  public List<Pair<String, String>> getFacetCounts(String facetName) {
    return null;
  }
}
