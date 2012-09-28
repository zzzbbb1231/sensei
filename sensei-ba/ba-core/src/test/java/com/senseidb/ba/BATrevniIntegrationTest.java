package com.senseidb.ba;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.senseidb.ba.trevni.impl.TrevniReaderImpl;
import com.senseidb.util.SingleNodeStarter;

public class BATrevniIntegrationTest extends TestCase {
  
  
  
  
  @Before
  public void setUp() throws Exception {
    File indexDir = new File("trevniIndex"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
    indexDir = new File(indexDir, "trevni");
    indexDir.mkdir();
    File avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    //DataMaker.createTrevniFilesForAndReturnSchema(avroFile, indexDir.getAbsolutePath());
    
    
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf-trevni").toURI());
    
    SingleNodeStarter.start(ConfDir1, 10000);
  }
  @After
  public void tearDown() throws Exception {
    File indexDir = new File("trevniIndex"); 
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
        "   ]," +
        "    \"facets\": {\n" + 
        "        \"dim_memberCompany\": {\n" + 
        "            \"max\": 10,\n" + 
        "            \"minCount\": 1,\n" + 
        "            \"expand\": false,\n" + 
        "            \"order\": \"hits\"\n" + 
        "        }\n" + 
        "    }" + 
        "}";
      
     JSONObject resp = search(new URL("http://localhost:8076/sensei"), new JSONObject(req).toString());
     System.out.println(resp.toString(1));
     assertEquals("numhits is wrong", 2807, resp.getInt("numhits"));
  }
  public static JSONObject search(URL url, String req) throws Exception {
    URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    String reqStr = req;
    System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while((line = reader.readLine()) != null)
      sb.append(line);
    String res = sb.toString();
    // System.out.println("res: " + res);
    JSONObject ret = new JSONObject(res);
    if (ret.opt("totaldocs") !=null){
     // assertEquals(15000L, ret.getLong("totaldocs"));
    }
    return ret;
  }
}
