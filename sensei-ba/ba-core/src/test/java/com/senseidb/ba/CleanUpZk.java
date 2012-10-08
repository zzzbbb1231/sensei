package com.senseidb.ba;

import org.I0Itec.zkclient.ZkClient;

import com.senseidb.ba.management.ZkManager;

public class CleanUpZk {
  private static ZkManager zkClient;

  public static void main(String[] args) {
    zkClient = new ZkManager("localhost:2181", "ba-server");
    zkClient.removePartition(0);
    zkClient.removePartition(1);
  }
}
