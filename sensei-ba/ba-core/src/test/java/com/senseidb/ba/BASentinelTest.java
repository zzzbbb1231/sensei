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

public class BASentinelTest extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
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
    // System.setProperty("com.linkedin.norbert.disableJMX", "true");
    indexDir = new File("testIndex");
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/partitions/testCluster2");
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf").toURI());
    httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    // createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    indexSegmentImpl = TestUtil.createIndexSegment();
    for (int i = 0; i < 2; i++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      FileInputStream inputStream = new FileInputStream(compressedFile);
      String port = "8088";
      FileUploadUtils.sendFile("localhost", port, "segment" + i, inputStream, compressedFile.length());
      IOUtils.closeQuietly(inputStream);
    }
    for (int i = 0; i < 2; i++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      FileInputStream inputStream = new FileInputStream(compressedFile);
      // testing http and the file system
      String port = i == 0 ? "8088" : "7088";
      FileUploadUtils.sendFile("localhost", port, "segment" + i, inputStream, compressedFile.length());
      IOUtils.closeQuietly(inputStream);
    }
    SingleNodeStarter.waitTillServerStarts(20000);
  }

  @Test
  public void test1FilterAndFacetCountOnNotSortedColumn() throws Exception {
    // Thread.sleep(1000000);
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 0, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 4444, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 4808, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 20000, resp.getInt("numhits"));
  }

  @Test
  public void test5RangeQueryOnSingleValuedColumn() throws Exception {
    String req = "{" +
        "  " +
        "    \"from\": 0," +
        "    \"size\": 10,\n" +
        "    \"selections\": [" +
        "    {" +
        "        \"terms\": {" +
        "            \"dim_memberGender\": {" +
        "                \"values\": [\"[m TO n)\"]," +
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
    assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit", 20000, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    // since creative id is only 1, not running an inclusive query results in 0 responses, which is correct
    assertEquals("all documents are a part of the hit", 0, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    // since creative id is only 1, not running an inclusive query results in 0 responses, which is correct
    assertEquals("all documents are a part of the hit", 19998, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit", 0, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit", 2, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit", 6, resp.getInt("numhits"));
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
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("all documents are a part of the hit", 19994, resp.getInt("numhits"));
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
        "    \"size\": 0,\n" +
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

  @Test
  public void test9ManagementBackend() throws Exception {
    String stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/segments/");
    JSONObject json = new JSONObject(stringResponse);
    String[] names = JSONObject.getNames(json);
    Arrays.sort(names);
    assertEquals("[0, 1]", Arrays.toString(names));
    names = JSONObject.getNames(json.getJSONObject("0"));
    Arrays.sort(names);
    assertEquals("[segment1]", Arrays.toString(names));
  }

  @Test
  public void test10MaxMapReduce() throws Exception {
    String req = "{\"filter\":{\"term\":{\"dim_memberGender\":\"m\"}}"
        + ", \"mapReduce\":{\"function\":\"sensei.max\",\"parameters\":{\"column\":\"met_impressionCount\"}}}";

    JSONObject res = TestUtil.search(new URL("http://localhost:8075/sensei"), req);
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");

    assertEquals(53, Double.valueOf(Double.parseDouble(mapReduceResult.getString("max"))).longValue());

  }

  @Test
  public void test11Avg() throws Exception {

    String req = "{\"filter\":{\"term\":{\"dim_memberGender\":\"m\"}}" +
        ", \"mapReduce\":{\"function\":\"sensei.avg\",\"parameters\":{\"column\":\"met_impressionCount\"}}}";
    JSONObject res = TestUtil.search(new URL("http://localhost:8075/sensei"), req);
    System.out.println(res.toString(1));
    JSONObject mapReduceResult = res.getJSONObject("mapReduceResult");
    assertEquals(2.48, mapReduceResult.getDouble("avg"), 0.1);
    assertEquals(13222, Long.parseLong(mapReduceResult.getString("count")));
  }

  @Test
  public void test12SimpleBQL() throws Exception {
    String req = "{\"bql\":\"select * from sensei where dim_memberIndustry in (102, 100)\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 326, resp.getInt("numhits"));

  }

  @Test
  public void test13AggregateBQL() throws Exception {
    String req = "{\"bql\":\"select sum(met_impressionCount) where dim_memberIndustry in (102, 100)\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 326, resp.getInt("numhits"));
    assertEquals(812, resp.getJSONObject("mapReduceResult").getInt("sum"));

  }

  @Test
  public void test14AggregateBQLWithGroupBy() throws Exception {
    String req = "{\"bql\":\"select avg(met_impressionCount) where dim_memberIndustry in (102, 100) group by dim_memberIndustry\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 326, resp.getInt("numhits"));
    assertEquals("2.50327", resp.getJSONObject("mapReduceResult").getJSONArray("grouped").getJSONObject(0).getString("avg"));
  }

  @Test
  public void test15AggregateBQLOnFullDataSet() throws Exception {
    String req = "{\"bql\":\"select sum(met_impressionCount) \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(20000, resp.getInt("numhits"));
    assertEquals(49138, resp.getJSONObject("mapReduceResult").getInt("sum"));
  }

  @Test
  public void test16AggregateBQLWithRangePredicate() throws Exception {
    String req = "{\"bql\":\"select * where dim_memberIndustry <= 100 \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(784, resp.getInt("numhits"));
  }

  @Test
  public void test16AggregateBQLOnFullDataSet() throws Exception {
    String req = "{\"bql\":\"select sum(met_impressionCount) \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(20000, resp.getInt("numhits"));
    assertEquals(49138, resp.getJSONObject("mapReduceResult").getInt("sum"));
  }

  @Test
  public void test17RangeQueryBQLOnSortedColumn() throws Exception {
    String req = "{\"bql\":\"select avg(met_impressionCount),max(met_impressionCount) \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 20000, resp.getInt("numhits"));

  }

  @Test
  public void test18RangeQueryBQLOnSortedColumn() throws Exception {

    String req = "{\"bql\":\"select * where dim_memberAge > 5000000\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 15278, resp.getInt("numhits"));

  }

  @Test
  public void test19SortedUniqueValues() throws Exception {

    String req = "{\"bql\":\"select * where dim_memberAge > 5000000 EXECUTE(com.senseidb.ba.mapred.impl.SortedUniqueValues, 'valueColumn':'met_impressionCount', 'sortColumn':'met_impressionCount', 'offset':'0', 'size':'100')\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 15278, resp.getInt("numhits"));

  }

  @Test
  public void test20SortedForwardIndex() throws Exception {

    String req = "{\"bql\":\"select sum(met_impressionCount) where dim_memberAge <= 5000000\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei/federatedBroker/"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(4722, resp.getInt("numhits"));
    assertEquals(11954, resp.getJSONObject("mapReduceResult").getInt("sum"));
  }

  @Test
  public void test1TestIndexMonitor() throws Exception {
    String req = "{\"bql\":\"select com.senseidb.ba.monitor.IndexMonitorMapReduce(*)\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei/"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(180000, resp.getJSONObject("mapReduceResult").getLong("allSegments_totalDocumentsCount_invertedIndex"));
    assertEquals(402352, resp.getJSONObject("mapReduceResult").getLong("allSegments_memoryConsumption_invertedIndex"));
    assertEquals(100410, resp.getJSONObject("mapReduceResult").getLong("allSegments_documentsCount_invertedIndex"));
    assertEquals(201176, resp.getJSONObject("mapReduceResult").getJSONArray("allSegments__indexMonitor").getJSONObject(0).getLong("segment_memoryConsumption_invertedIndex_total"));
    assertEquals(205, resp.getJSONObject("mapReduceResult").getJSONArray("allSegments__indexMonitor").getJSONObject(0).getLong("segment_documentsCount_invertedIndex_standardCardinality"));
    assertEquals(50000, resp.getJSONObject("mapReduceResult").getJSONArray("allSegments__indexMonitor").getJSONObject(0).getLong("segment_documentsCount_invertedIndex_highCardinality"));
    assertEquals(1176, resp.getJSONObject("mapReduceResult").getJSONArray("allSegments__indexMonitor").getJSONObject(0).getLong("segment_memoryConsumption_invertedIndex_standardCardinality"));
    assertEquals("segment0", resp.getJSONObject("mapReduceResult").getJSONArray("allSegments__indexMonitor").getJSONObject(0).getString("segment_name"));
  }

}
