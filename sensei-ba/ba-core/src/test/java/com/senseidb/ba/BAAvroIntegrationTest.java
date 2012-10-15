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

import com.senseidb.ba.gazelle.creators.SegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class BAAvroIntegrationTest extends TestCase {
  
   
  
  
  @Before
  public void setUp() throws Exception {
    File indexDir = new File("avroIndex"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
    indexDir = new File(indexDir, "segment1");
    indexDir.mkdir();
    File avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    GazelleIndexSegmentImpl indexSegmentImpl =  SegmentCreator.readFromAvroFile(avroFile);
    com.senseidb.ba.gazelle.persist.SegmentPersistentManager.flushToDisk(indexSegmentImpl, indexDir);
    FileUtils.copyFileToDirectory(avroFile, new File("avroIndex"));
    
    
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf-avro").toURI());
    
    SingleNodeStarter.start(ConfDir1, 20000);
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
     assertEquals("numhits is wrong", 13222, resp.getInt("numhits"));
  }
  /*@Test
  public void test2() throws Exception  {
    Thread.sleep(Long.MAX_VALUE);
  }*/
 
}
