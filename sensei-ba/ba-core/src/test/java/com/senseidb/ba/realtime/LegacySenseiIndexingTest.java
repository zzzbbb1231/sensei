package com.senseidb.ba.realtime;

import java.io.File;
import java.net.URL;

import junit.framework.Assert;


import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.senseidb.ba.BASentinelTest;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class LegacySenseiIndexingTest  {
  @BeforeClass
  public static void setUp() throws Exception {
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("legacy-config").toURI());
    Logger rootLogger = Logger.getRootLogger();
    SingleNodeStarter.rmrf(new File("/tmp/legacyIndex"));
    //rootLogger.setLevel(Level.WARN);
    SingleNodeStarter.start(ConfDir1, 14999);
    
  }
  @Test
  public void test1() throws Exception {
   
   
            String req = "{\"bql\":\"select * where tags = 'electric' \"}";
            JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
            Assert.assertEquals("numhits is wrong", 2349, resp.getInt("numhits"));
  
  }
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.rmrf(new File("/tmp/legacyIndex"));
    SingleNodeStarter.shutdown(); 
    //SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    //FileUtils.deleteDirectory(new File("ba-index/ba-data"));
  }
 
}
