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
public class HttpLoadTest  {
  @BeforeClass
  public static void setUp() throws Exception {
    SingleNodeStarter.rmrf(new File("/tmp/realtimeIndexHttp/test"));
    File ConfDir1 = new File("/home/vzhabiuk/work/sensei-ba/sensei/sensei-ba/config-example/src/main/resources/realtime-config-cluster-http/node1");
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);
    SingleNodeStarter.start(ConfDir1, 100000000);
    
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
            String req = "{\"bql\":\"select * \"}";
            JSONObject resp = TestUtil.search(new URL("http://localhost:8077/sensei"), new JSONObject(req).toString());
            /*if (System.currentTimeMillis() - time > 25000) {
              break;
            }*/
            if (ext == 0 && count %10 == 0) {
              System.out.println("numhits = " + resp.getInt("numhits"));
            }
            Thread.sleep(500);
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
    SingleNodeStarter.rmrf(new File("/tmp/realtimeIndexHttp/test"));
    System.out.println("!!Time to shutdown = " + (System.currentTimeMillis() - time));
    //SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    //FileUtils.deleteDirectory(new File("ba-index/ba-data"));
  }
 
}
