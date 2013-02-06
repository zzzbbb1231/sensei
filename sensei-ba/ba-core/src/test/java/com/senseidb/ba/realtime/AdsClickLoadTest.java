package com.senseidb.ba.realtime;

import java.io.File;
import java.net.URL;


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
@Ignore
public class AdsClickLoadTest  {
  @BeforeClass
  public static void setUp() throws Exception {
    File ConfDir1 = new File("/home/vzhabiuk/work/sensei-ba/sensei/sensei-ba/ba-core/src/test/resources/realtime-config");
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);
    SingleNodeStarter.start(ConfDir1, 100);
    
  }
  @Test
  public void test9ManagementBackend() throws Exception {
   
    Thread[] thread = new Thread[15];
    for (int i = 0; i < thread.length; i++) {
      final int ext = i;  
      thread[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
          int count  = 0;
          long time = System.currentTimeMillis();
          while (true) {
            String req = "{\"bql\":\"select max(clickCount) where campaignId <> 628 and memberFunctions <> 'admn'  \"}";
            JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
            /*if (System.currentTimeMillis() - time > 25000) {
              break;
            }*/
            if (ext == 0 && count %10 == 0) {
              System.out.println("numhits = " + resp.getInt("numhits"));
            }
            //System.out.println("errors = " + resp.getJSONArray("errors"));
            
          }
          
          
          
          } catch (Exception ex) {
            ex.printStackTrace();
          }
          
        }
      });
      thread[i].start();
      
    }
    for (int i = 0; i < thread.length; i++) {
    thread[i].join();
    }
    
  
  }
  @AfterClass
  public static void tearDown() throws Exception {
    System.out.println("!!!tear down");
    //Thread.sleep(5000);
    long time = System.currentTimeMillis();
    SingleNodeStarter.shutdown(); 
    System.out.println("!!Time to shutdown = " + (System.currentTimeMillis() - time));
    //SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    //FileUtils.deleteDirectory(new File("ba-index/ba-data"));
  }
 
}
