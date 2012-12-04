package com.senseidb.ba.management;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.plugins.ZeusIndexReaderDecorator;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;

public class BaIndexFactoryManager implements SenseiPlugin, ZoieFactoryFactory {
  private static Logger logger = Logger.getLogger(BaIndexFactoryManager.class);
  private String zookeeperUrl;
  private String hdfsUrl;
  private ZkClient zkClient;
  private FileSystem fileSystem;
  private ExecutorService executorService;
  private volatile boolean stopped;
  private String clusterName;
  private SenseiPluginRegistry pluginRegistry;
  private ZeusIndexReaderDecorator zeusIndexReaderDecorator;
  private ReadMode readMode;
  @SuppressWarnings( { "unchecked", "rawtypes" })
  @Override
  public SenseiZoieFactory<?> getZoieFactory(final File idxDir, ZoieIndexableInterpreter<?> interpreter, final SenseiIndexReaderDecorator decorator,
      ZoieConfig config) {
    return new SenseiZoieFactory( idxDir, null, interpreter, decorator, config) {
      @Override
      public Zoie getZoieInstance(int nodeId, int partitionId) {
        return new BaIndexFactory(SenseiZoieFactory.getPath(idxDir, nodeId, partitionId), clusterName, zeusIndexReaderDecorator, zkClient, fileSystem, readMode,nodeId, partitionId, executorService);
      }
      @Override
      public File getPath(int nodeId, int partitionId) {
        return SenseiZoieFactory.getPath(idxDir, nodeId, partitionId);
      }
    };
  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.pluginRegistry = pluginRegistry;
    try {
    zookeeperUrl = pluginRegistry.getConfiguration().getString("sensei.cluster.url");
    zkClient = new ZkClient(zookeeperUrl);
    hdfsUrl = pluginRegistry.getConfiguration().getString("sensei.ba.hdfs.url");
    clusterName = pluginRegistry.getConfiguration().getString("sensei.cluster.name");
    int numberOfLoadingThreads = -1;
    zeusIndexReaderDecorator = new ZeusIndexReaderDecorator(pluginRegistry.resolveBeansByListKey(SenseiPluginRegistry.FACET_CONF_PREFIX, FacetHandler.class));
    if (config.containsKey("numLoadingThreads")) {
      numberOfLoadingThreads = Integer.parseInt(config.get("numLoadingThreads"));
    }
    if (numberOfLoadingThreads > 0) {
      executorService = java.util.concurrent.Executors.newFixedThreadPool(numberOfLoadingThreads);
    }
    if (hdfsUrl != null) {
      Configuration fsConfig = new Configuration();
      fsConfig.set("fs.default.name",hdfsUrl);//"hdfs://127.0.0.1:9000/");
        fileSystem = FileSystem.get(fsConfig);
      
    }
    String readModeStr = config.get("readMode");
    if (readModeStr != null && ReadMode.valueOf(readModeStr) != null) {
      
      readMode = ReadMode.valueOf(readModeStr);
      logger.info("Initialized the readmode from the configuration - " + readMode);
    } else {
      readMode = ReadMode.DirectMemory;
      logger.info("Initialized the default readmode - " + readMode);
    }
    Runtime.getRuntime().removeShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
        BaIndexFactoryManager.this.stop();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    });
    } catch (IOException e) {
     throw new RuntimeException();
    }
  }

  @Override
  public void start() {
    
  }

  @Override
  public void stop() {
    if (!stopped) {
      stopped = true;
      zkClient.unsubscribeAll();
      zkClient.close();
    }
    
  }

}
