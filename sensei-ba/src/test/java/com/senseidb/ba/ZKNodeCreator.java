package com.senseidb.ba;

import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZKNodeCreator {
  private static final String ZOOKEEPER_PATH_FOR_THE_PARTIOTION = "/sensei-ba/paritions";
  private static final int TIMEOUT = 3000;
  private Map<String, Boolean> previouslyKnownServerStatus = new HashMap<String, Boolean>();
  private ZkClient zkClient;
  public ZKNodeCreator() throws Exception {
    zkClient = new ZkClient("localhost:2181", TIMEOUT);
    zkClient.deleteRecursive(ZOOKEEPER_PATH_FOR_THE_PARTIOTION);
    zkClient.createPersistent(ZOOKEEPER_PATH_FOR_THE_PARTIOTION, true);
   
    int i = 0;
    while(true) {
      zkClient.createPersistent(ZOOKEEPER_PATH_FOR_THE_PARTIOTION + "/" + i, true);
     i++;
      
    }
  }
  public static void main(String[] args) throws Exception {
    new ZKNodeCreator();
    Thread.sleep(Long.MAX_VALUE);
    
  }
 
    

}