package com.senseidb.ba.realtime;

import java.net.URL;

import org.json.JSONObject;

import com.senseidb.ba.util.TestUtil;

public class Main {

  /**
   * @param args
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws InterruptedException {
    Thread[] thread = new Thread[35];
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
            JSONObject resp = TestUtil.search(new URL("http://localhost:8892/sensei"), new JSONObject(req).toString());
            /*if (System.currentTimeMillis() - time > 25000) {
              break;
            }*/
            if (ext == 0 && count %10 == 0) {
              System.out.println("numhits = " + resp.getInt("numhits"));
            }
            //Thread.sleep(500);
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

}
