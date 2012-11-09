package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.file.http.JettyServerHolder;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class BASentinelTest  extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
  private static File compressedSegment;
  private static GazelleIndexSegmentImpl indexSegmentImpl;
  private static JettyServerHolder jettyServerHolder;
  private static String httpUploadDirectory;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    jettyServerHolder.stop();
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf").toURI());
    jettyServerHolder = new JettyServerHolder();
    jettyServerHolder.setPort(8088);
     httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    indexSegmentImpl = TestUtil.createIndexSegment();
    for (int i = 0; i < 2; i++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      FileInputStream inputStream = new FileInputStream(compressedFile);
      FileUploadUtils.sendFile("localhost", "8088", "segment" + i, inputStream, compressedFile.length());
      IOUtils.closeQuietly(inputStream);
    }
    SingleNodeStarter.waitTillServerStarts(20000);

  }

  public static void createAndLaunchJettyServer() {
    jettyServerHolder.setDirectoryPath(httpUploadDirectory);
    jettyServerHolder.setClusterName("testCluster2");
    jettyServerHolder.setMaxPartitionId(1);
    jettyServerHolder.setZkUrl("localhost:2181");
    jettyServerHolder.setBaseUrl("http://localhost:8088/files/");
    jettyServerHolder.start();
  }

  @Test
  public void test1FilterAndFacetCountOnNotSortedColumn() throws Exception {

    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberGender\": {" + 
        "                \"values\": [\"m\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"," + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ], " +
        "\"sort\": [{\"dim_memberCompany\": \"desc\"}]," +
        "    \"facets\": {\n" + 
        "        \"dim_memberCompany\": {\n" + 
        "            \"max\": 10,\n" + 
        "            \"minCount\": 1,\n" + 
        "            \"expand\": false,\n" + 
        "            \"order\": \"hits\",\n" + 
        " \"properties\":{\"maxFacetsPerKey\":1}" +
        "        }\n" + 
        "    }" + 
        "}";
      
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }

  @Test
  public void test2FilterBySortedColumn() throws Exception {

    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"shrd_advertiserId\": {" + 
        "                \"values\": [\"-400\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
       
        "    }" + 
        "}";
      
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 4, resp.getInt("numhits"));
  }

  @Test
  public void test3FilterAndFacetBySortedColumn() throws Exception {

  String req = "{" + 
      "  " + 
      "    \"from\": 0," + 
      "    \"size\": 10,\n" + 
      "    \"selections\": [" + 
      "    {" + 
      "        \"terms\": {" + 
      "            \"dim_memberGender\": {" + 
      "                \"values\": [\"m\"]," + 
      "                \"excludes\": []," + 
      "                \"operator\": \"or\"" + 
      "            }" + 
      "        }" + 
      "    }" + 
      "   ], \"sort\": [{\"dim_memberCompany\": \"desc\"}]," +
      "    \"facets\": {\n" + 
      "        \"shrd_advertiserId\": {\n" + 
      "            \"max\": 10,\n" + 
      "            \"minCount\": 1,\n" + 
      "            \"expand\": false,\n" + 
      "            \"order\": \"hits\"\n" + 
      "        }\n" + 
      "    }" + 
      "}";
    
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }

  @Test
  public void test4FilterOnMultiColumn() throws Exception {

    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"3\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
        "}";
      
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 6, resp.getInt("numhits"));
  }

  @Test
  public void test5FacetByMultiColumn() throws Exception {
    
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 0,\n" + 
        "    \"facets\": {\n" + 
        "        \"dim_skills\": {\n" + 
        "            \"max\": 10,\n" + 
        "            \"minCount\": 1,\n" + 
        "            \"expand\": false,\n" + 
        "            \"order\": \"hits\"\n" + 
        "        }\n" + 
        "    }" + 
        "}";
      
     JSONObject resp = null;
     for (int i = 0; i < 2; i ++) {
       resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
     }
     JSONObject facetValue = resp.getJSONObject("facets").getJSONArray("dim_skills").getJSONObject(0);
     assertEquals("0000000003", facetValue.getString("value"));
     assertEquals(6, facetValue.getInt("count"));
  }

  @Test
  public void test1RangeQueryOnSingleValuedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberRegion\": {" + 
        "                \"values\": [\"[84 TO 4618]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong",0 , resp.getInt("numhits"));
  }

  @Test
  public void test2RangeQueryOnSingleValuedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberRegion\": {" + 
        "                \"values\": [\"(4613 TO 4910)\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong",4444 , resp.getInt("numhits"));
  }
  
  @Test
  public void test3RangeQueryOnSingleValuedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberRegion\": {" + 
        "                \"values\": [\"(4613 TO 4910]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong",4808 , resp.getInt("numhits"));
  }
  
  @Test
  public void test4RangeQueryOnSingleValuedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberRegion\": {" + 
        "                \"values\": [\"[* TO *]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong",20000 , resp.getInt("numhits"));
  }
  
  
  @Test
  public void test1RangeQueryOnSortedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_creativeId\": {" + 
        "                \"values\": [\"[134006 TO 134006]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit",20000 , resp.getInt("numhits"));
  }
  
  @Test
  public void test2RangeQueryOnSortedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_creativeId\": {" + 
        "                \"values\": [\"(134006 TO 134006)\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    // since creative id is only 1, not running an inclusive query results in 0 responses, which is correct
    assertEquals("all documents are a part of the hit",0 , resp.getInt("numhits"));
  }
  
  @Test
  public void test3RangeQueryOnSortedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"shrd_advertiserId\": {" + 
        "                \"values\": [\"[-400 TO *)\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    // since creative id is only 1, not running an inclusive query results in 0 responses, which is correct
    assertEquals("all documents are a part of the hit",10 , resp.getInt("numhits"));
  }
  
  @Test
  public void test1RangeQueryOnMultivaluedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"[20 TO 40]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit",0 , resp.getInt("numhits"));
  }
  
  @Test
  public void test2RangeQueryOnMultivaluedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"(1 TO 3)\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit",2 , resp.getInt("numhits"));
  }
  
  @Test
  public void test3RangeQueryOnMultivaluedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"(1 TO 3]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit",6 , resp.getInt("numhits"));
  }
  
  @Test
  public void test4RangeQueryOnMultivaluedColumn() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"[* TO 1)\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +
 
        "    }" + 
        "}";

    JSONObject resp = null;
    for (int i = 0; i < 2; i ++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit",19994 , resp.getInt("numhits"));
  }
  
  @Test
  public void test6SumGroupBy() throws Exception {
  
  String req = "{" + 
      "  " + 
      "    \"from\": 0," + 
      "    \"size\": 10,\n" + 
      "    \"selections\": [" + 
      "    {" + 
      "        \"terms\": {" + 
      "            \"dim_memberGender\": {" + 
      "                \"values\": [\"m\"]," + 
      "                \"excludes\": []," + 
      "                \"operator\": \"or\"," + 
      "            }" + 
      "        }" + 
      "    }" + 
      "   ], " +
      "    \"facets\": {\n" + 
      "        \"sumGroupBy\": {\n" + 
      "            \"max\": 10,\n" + 
      "            \"minCount\": 1,\n" + 
      "            \"expand\": false,\n" + 
      "            \"order\": \"hits\",\n" + 
      " \"properties\":{\"dimension\":\"shrd_advertiserId\", \"metric\":\"met_impressionCount\"}" +
      "        }\n" + 
      "    }" + 
      "}";
    
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("2", resp.getJSONObject("facets").getJSONArray("sumGroupBy").getJSONObject(3).getString("count"));
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }
  @Test
  public void test6SumGroupByMultiValue() throws Exception {
  
  String req = "{" + 
      "  " + 
      "    \"from\": 0," + 
      "    \"size\": 10,\n" + 
      "    \"selections\": [" + 
      "    {" + 
      "        \"terms\": {" + 
      "            \"dim_memberGender\": {" + 
      "                \"values\": [\"m\"]," + 
      "                \"excludes\": []," + 
      "                \"operator\": \"or\"," + 
      "            }" + 
      "        }" + 
      "    }" + 
      "   ], " +
      "    \"facets\": {\n" + 
      "        \"sumGroupBy\": {\n" + 
      "            \"max\": 10,\n" + 
      "            \"minCount\": 1,\n" + 
      "            \"expand\": false,\n" + 
      "            \"order\": \"hits\",\n" + 
      " \"properties\":{\"dimension\":\"dim_skills\", \"metric\":\"met_impressionCount\"}" +
      "        }\n" + 
      "    }" + 
      "}";
    
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
   
    System.out.println(resp.toString(1));
    assertEquals("2", resp.getJSONObject("facets").getJSONArray("sumGroupBy").getJSONObject(3).getString("count"));
    assertEquals("0000000002", resp.getJSONObject("facets").getJSONArray("sumGroupBy").getJSONObject(3).getString("value"));
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }
  @Test
  public void test7Sum() throws Exception {
  
  String req = "{" + 
      "  " + 
      "    \"from\": 0," + 
      "    \"size\": 10,\n" + 
      "    \"selections\": [" + 
      "    {" + 
      "        \"terms\": {" + 
      "            \"dim_memberGender\": {" + 
      "                \"values\": [\"m\"]," + 
      "                \"excludes\": []," + 
      "                \"operator\": \"or\"," + 
      "            }" + 
      "        }" + 
      "    }" + 
      "   ], " +
      "    \"facets\": {\n" + 
      "        \"sum\": {\n" + 
      "            \"max\": 10,\n" + 
      "            \"minCount\": 1,\n" + 
      "            \"expand\": false,\n" + 
      "            \"order\": \"hits\",\n" + 
      " \"properties\":{\"column\":\"met_impressionCount\"}" +
      "        }\n" + 
      "    }" + 
      "}";
    
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("32830", resp.getJSONObject("facets").getJSONArray("sum").getJSONObject(0).getString("count"));
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }
  @Test
  public void test8FilterOrQuery() throws Exception {
    String req = "{\"filter\":{\"or\":[{\"term\":{\"dim_memberGender\":\"f\"}}]}}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    assertEquals("numhits is wrong", 5614, resp.getInt("numhits"));
     req = "{\"filter\":{\"or\":[{\"term\":{\"dim_memberGender\":\"f\"}},{\"term\":{\"shrd_advertiserId\":\"-500\"}}]}}";
     resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    assertEquals("numhits is wrong", 5616, resp.getInt("numhits"));
  }
 
  
}
