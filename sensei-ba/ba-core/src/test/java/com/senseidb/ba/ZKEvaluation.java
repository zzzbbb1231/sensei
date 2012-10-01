package com.senseidb.ba;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKEvaluation  {

  private static final String ZOOKEEPER_PATH_FOR_THE_PARTIOTION = "/sensei-ba/paritions";
  private static final int TIMEOUT = 3000;
  private ZkClient zkClient;
  public ZKEvaluation() throws IOException {
    zkClient = new ZkClient("localhost:2181", TIMEOUT);
  
    zkClient.subscribeChildChanges(ZOOKEEPER_PATH_FOR_THE_PARTIOTION, new IZkChildListener() {
      
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        System.out.println("currentChilds = " + currentChilds.size());
        
      }
    });
  }
public static void main(String[] args) throws Exception {
  new ZKEvaluation();
  Thread.sleep(Long.MAX_VALUE);
}

}
