package com.senseidb.ba.management.controller;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;


public abstract class BaController implements SenseiPlugin {
  protected  Logger logger = Logger.getLogger(getClass());  
  private static volatile ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("SenseiBAControllerThread");
        return thread;
      }
    });
  
  
    protected ZkClient zkClient;
    private long frequency;
    protected String controllerName;
    private volatile boolean isMaster;
    protected String clusterName;
    protected String nodeId;
    private Map<String, String> config;
    private SenseiPluginRegistry pluginRegistry;
     /**
     * Specifies what is the delay in seconds between the executions of the execute method
     * @return
     */
    public long getExecutionFrequency(SenseiPluginRegistry pluginRegistry, Map<String,String> config) {
      if (!config.containsKey("frequency")) {
        throw new IllegalStateException("Property 'frequency' is missing for the controller");
      }
      return Long.parseLong(config.get("frequency"));
    }
    /**
     * Specifies what is the delay in seconds between the executions of the execute method
     * @return
     */
    public String getControllerName(SenseiPluginRegistry pluginRegistry, Map<String,String> config) {
      if (!config.containsKey("name")) {
        throw new IllegalStateException("Property 'name' is missing for the controller");
      }
      return config.get("name");
    }
    /**
     * The callback method that allows to do back ground tasks
     */
    public abstract void execute();
    
    public final void start() {
    
      long executionFrequency = getExecutionFrequency(pluginRegistry, config);
      if (executorService == null || executorService.isShutdown()) {
        executorService = Executors.newScheduledThreadPool(1, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName("SenseiBAControllerThread");
            return thread;
          }
        });
      }
      executorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          checkIfMasterAndExecute();
          
        }
      }, 5, executionFrequency , TimeUnit.SECONDS);
    }
    protected abstract  void doStop();
    public final void stop() {
      if (isMaster) {      
        String path = SegmentUtils.getMasterZkPath(clusterName) + "/" + controllerName;
        if (zkClient.exists(path)) {
          try {
            zkClient.deleteRecursive(path);
          } catch (Exception ex) {
            logger.error("Couldn't remove the master lock", ex);
          }
        }
      }
      doStop();
      synchronized (executorService) {
      if (!executorService.isShutdown()) {
        executorService.shutdown();
      }
      
      }
    }
    @Override
    public final void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
       this.config = config;
      this.pluginRegistry = pluginRegistry;
      clusterName = config.get("clusterName");
      if (clusterName == null) {
        clusterName = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
      }
      Assert.notNull(clusterName, "clusterName parameter should be present");
      String zkUrl = config.get("zkUrl");         
      if (zkUrl == null) { 
        zkUrl = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_URL);
      }
      Assert.notNull(zkUrl, "zkUrl parameter should be present");
      int zkTimeout = pluginRegistry.getConfiguration().getInt(SenseiConfParams.SENSEI_CLUSTER_TIMEOUT, 300000);
      zkClient = new ZkClient(zkUrl, zkTimeout);
      zkClient.setZkSerializer(new BytesPushThroughSerializer());
      frequency = getExecutionFrequency(pluginRegistry, config); 
      controllerName = getControllerName(pluginRegistry, config);
      
      nodeId = config.get("nodeId");      
      if (nodeId == null) {
        nodeId = pluginRegistry.getConfiguration().getString(SenseiConfParams.NODE_ID);
      }
      Assert.notNull(nodeId, "nodeId parameter should be present");
    }
    
    public synchronized final void checkIfMasterAndExecute () {
      try {
     
      if (isMaster) {        
          execute();        
      } else {
        
        String path = SegmentUtils.getMasterZkPath(clusterName) + "/" + controllerName;
        if (zkClient.exists(path)) {
          logger.debug("The path " + path + " so will not create an ephemeral node" );
        } else {
          try {
            if (!zkClient.exists(SegmentUtils.getMasterZkPath(clusterName))) {
              zkClient.createPersistent(SegmentUtils.getMasterZkPath(clusterName), true);
            }
            zkClient.createEphemeralSequential(path, new MasterInfo(new SimpleDateFormat("dd MMM yyyy hh:mm:ss zzz").format(new Date()) , nodeId).toBytes());
            logger.info("Becoming a master with nodeId = " + nodeId);
            isMaster = true;
            doBecomeMaster(config, pluginRegistry);
            execute();
          } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
          }
        }
      }
      
      } catch (Exception ex) {
        logger.error("Error in controller background thread");
      }
    }
    protected abstract void doBecomeMaster(Map<String, String> config, SenseiPluginRegistry pluginRegistry);
    
}
