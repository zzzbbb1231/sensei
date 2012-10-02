package com.senseidb.ba.management;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;

public class BaIndexFactoryManager implements SenseiPlugin, ZoieFactoryFactory {

  private String zookeeperUrl;
  private String hdfsUrl;
  private ZkClient zkClient;
  private FileSystem fileSystem;
  
  @SuppressWarnings( { "unchecked", "rawtypes" })
  @Override
  public SenseiZoieFactory<?> getZoieFactory(final File idxDir, ZoieIndexableInterpreter<?> interpreter, final SenseiIndexReaderDecorator decorator,
      ZoieConfig config) {
    return new SenseiZoieFactory( idxDir, null, interpreter, decorator, config) {
      @Override
      public Zoie getZoieInstance(int nodeId, int partitionId) {
        return new BaIndexFactory(SenseiZoieFactory.getPath(idxDir, nodeId, partitionId), decorator, zkClient, fileSystem, partitionId);
      }
      @Override
      public File getPath(int nodeId, int partitionId) {
        return SenseiZoieFactory.getPath(idxDir, nodeId, partitionId);
      }
    };
  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    try {
    zookeeperUrl = pluginRegistry.getConfiguration().getString("sensei.cluster.url");
    zkClient = new ZkClient(zookeeperUrl);
    hdfsUrl = pluginRegistry.getConfiguration().getString("sensei.ba.hdfs.url");
    if (hdfsUrl != null) {
      Configuration fsConfig = new Configuration();
      fsConfig.set("fs.default.name",hdfsUrl);//"hdfs://127.0.0.1:9000/");
        fileSystem = FileSystem.get(fsConfig);
      
    }
    } catch (IOException e) {
     throw new RuntimeException();
    }
  }

  @Override
  public void start() {
    
  }

  @Override
  public void stop() {
    
  }

}
