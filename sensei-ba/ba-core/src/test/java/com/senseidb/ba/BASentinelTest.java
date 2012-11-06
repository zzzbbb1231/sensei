package com.senseidb.ba;

import java.io.File;
import java.net.URL;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class BASentinelTest  extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
  private static File compressedSegment;
  private static GazelleIndexSegmentImpl indexSegmentImpl;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
   
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf").toURI());
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    indexSegmentImpl = TestUtil.createIndexSegment();
    for (int i = 0; i < 2; i++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      zkManager.registerSegment(i % 2, "segment" + i, compressedFile.getAbsolutePath(), SegmentType.COMPRESSED_GAZELLE,
          System.currentTimeMillis(), Long.MAX_VALUE);

    }
    SingleNodeStarter.waitTillServerStarts(20000);

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

  
  public void testRangeQueryOnSingleValuedColumn() throws Exception {
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

  public void testRangeQueryOnSortedColumn() throws Exception {
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
  
  public void testRangeQueryOnMultivaluedColumn() throws Exception {
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
    assertEquals("all documents are a part of the hit",6 , resp.getInt("numhits"));
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
