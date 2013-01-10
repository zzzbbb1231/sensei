package com.senseidb.ba;

import java.io.File;
import java.net.URL;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class BAStandaloneIntegrationTest extends TestCase {
  
  @Before
  public void setUp() throws Exception {
    File indexDir = new File("/tmp/static-ba"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
   
    File avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    File jsonFile = new File(getClass().getClassLoader().getResource("data/sample_data.json").toURI());
    File csvFile = new File(getClass().getClassLoader().getResource("data/sample_data.csv").toURI());   
    FileUtils.copyFileToDirectory(avroFile, indexDir);
    FileUtils.copyFileToDirectory(jsonFile, indexDir);
    FileUtils.copyFileToDirectory(csvFile, indexDir);
    File ConfDir1 = new File(BASegmentRecoverTest.class.getClassLoader().getResource("ba-conf-avro").toURI());
    
    SingleNodeStarter.start(ConfDir1, 10000);
  }
  @After
  public void tearDown() throws Exception {
    File indexDir = new File("avroIndex"); 
    SingleNodeStarter.rmrf(indexDir);
    SingleNodeStarter.shutdown(); 
  
  }
  
  public void test1() throws Exception {
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
        "        \"dim_memberCompany\": {\n" + 
        "            \"max\": 10,\n" + 
        "            \"minCount\": 1,\n" + 
        "            \"expand\": false,\n" + 
        "            \"order\": \"hits\"\n" + 
        "        }\n" + 
        "    }" + 
        "}";
      
     JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
     
     resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
     System.out.println(resp.toString(1));
     assertEquals("numhits is wrong", 19833, resp.getInt("numhits"));
  }
  
  public void test2() throws Exception {
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_skills\": {" + 
        "                \"values\": [\"5\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ]" +    
        "}";
      
     JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
 
     resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
     System.out.println(resp.toString(1));
     assertTrue("numhits is wrong" + resp.getInt("numhits"),  resp.getInt("numhits") >= 1 && resp.getInt("numhits") <= 2);
  }
  
  @Test
  public void test3SortColumnOnDifferentType() throws Exception {

    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 20,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"shrd_advertiserId\": {" + 
        "                \"values\": [\"[-500 TO -300]\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"," + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ], " +
        "\"sort\": [{\"dim_memberIndustry\": \"asc\"}]," +
        "}";
      
    JSONObject resp = null;
    for (int i = 0; i < 2; i++) {
      resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
    }
    System.out.println(resp.toString(1));
    assertEquals("numhits is wrong", 18, resp.getInt("numhits"));
  }
  
  /*@Test
  public void test2() throws Exception  {
    Thread.sleep(Long.MAX_VALUE);
  }*/
 
}
