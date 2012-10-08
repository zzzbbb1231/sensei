package com.senseidb.ba;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.senseidb.ba.management.ZkManager;

public class ZkCleaner {
public static void main(String[] args) throws Exception {
  ZkManager zkManager = new ZkManager("localhost:2181");
  zkManager.removePartition(0);
  zkManager.removePartition(1);
  FileUtils.deleteDirectory(new File("/tmp/ba-index"));
}
}
