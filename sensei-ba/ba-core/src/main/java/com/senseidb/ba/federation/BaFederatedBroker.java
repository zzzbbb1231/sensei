package com.senseidb.ba.federation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.api.BrowseSelection;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
import com.senseidb.ba.management.SegmentTracker;
import com.senseidb.cluster.routing.SenseiPartitionedLoadBalancerFactory;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.ResultMerger;
import com.senseidb.search.node.SenseiBroker;
import com.senseidb.search.node.broker.CompoundBrokerConfig;
import com.senseidb.search.node.broker.LayeredBroker;
import com.senseidb.search.node.broker.LayeredClusterPruner;
import com.senseidb.search.req.SenseiJSONQuery;
import com.senseidb.search.req.SenseiQuery;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.mapred.functions.MaxMapReduce;
import com.senseidb.svc.api.SenseiException;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

public class BaFederatedBroker extends LayeredBroker {
  private static Logger logger = Logger.getLogger(BaFederatedBroker.class);
  private static final Counter numRelevantHistoricalEvents = Metrics.newCounter(BaFederatedBroker.class, "numRelevantHistoricalEvents");
  private static final Counter numRelevantRealtimeEvents = Metrics.newCounter(BaFederatedBroker.class, "numfRelevantRealtimeEvents");
  private static final com.yammer.metrics.core.Timer historicalClusterTime = Metrics.newTimer(new MetricName(BaFederatedBroker.class ,"historicalClusterTime"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final com.yammer.metrics.core.Timer realtimeClusterTime = Metrics.newTimer(new MetricName(BaFederatedBroker.class ,"realtimeClusterTime"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final com.yammer.metrics.core.Timer mergeTime = Metrics.newTimer(new MetricName(BaFederatedBroker.class ,"mergeTime"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private static final Counter currentTimeBoundaryCounter = Metrics.newCounter(BaFederatedBroker.class, "currentTimeBoundaryCounter");
  
  private ExecutorService executorService = Executors.newCachedThreadPool();
  
  private Timer timer = new Timer(true);
  private static final String CLUSTERS = "clusters";
  private static final String HISTORICAL_CLUSTER = "historicalCluster";
  private static final String REFRESH_FREQUENCY = "refreshFrequency";
  private static final String TIME_COLUMN = "timeColumn";
  private String historicalCluster;
  private long frequency = 5 * 60 * 1000;
  private String timeColumn;
  private volatile boolean areClusterBoundariesDefined = false;
  private long timeBoundary = -1;
  private List<String> clusters = new ArrayList<String>();
  private Map<String, CompoundBrokerConfig> clusterBrokerConfig = new HashMap<String, CompoundBrokerConfig>() ;
  private Map<String, BrokerProxy> brokers = new HashMap<String, BrokerProxy>() ;
  private LayeredClusterPruner federatedPruner;
  private BrokerProxy historicalBroker;
  
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    try {
    System.setProperty("com.linkedin.norbert.disableJMX", "true");
    String clustersConfig = config.get(CLUSTERS);
    if (clustersConfig == null) {
      throw new IllegalArgumentException("Clusters param should be present");
    }
   
    PartitionedLoadBalancerFactory<String> routerFactory = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SERVER_SEARCH_ROUTER_FACTORY, PartitionedLoadBalancerFactory.class);
    if (routerFactory == null) {
      routerFactory = new SenseiPartitionedLoadBalancerFactory(1500);
    }
    for (String cluster : clustersConfig.split(",")) {
      String trimmed = cluster.trim();
      if (trimmed.length() > 0) {
        clusters.add(trimmed);
        clusterBrokerConfig.put(trimmed, new CompoundBrokerConfig(pluginRegistry.getConfiguration(), routerFactory, config, trimmed));
      }
    }
    Assert.state(config.containsKey(HISTORICAL_CLUSTER), "historicalCluster property is not defined");
    
    if (config.containsKey(REFRESH_FREQUENCY)) {
      frequency = Long.parseLong(config.get(REFRESH_FREQUENCY));
    }
    Assert.state(config.containsKey(TIME_COLUMN), "timeColumn property is not defined");
   historicalCluster = config.get(HISTORICAL_CLUSTER);
   timeColumn = config.get(TIME_COLUMN);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }
  
  @Override
  public void start() {
    try {
    for (String cluster : clusters) {
      CompoundBrokerConfig brokerConfig = clusterBrokerConfig.get(cluster);
      brokerConfig.init();
      brokers.put(cluster, new BrokerProxy(brokerConfig.getNetworkClient(), brokerConfig.getClusterClient(), true));
    }
    timer.schedule(new TimerTask() {
      
      @Override
      public void run() {
         try {
          refreshTime();
         } catch (Exception ex) {
           logger.error(ex.getMessage(), ex);
         }
        
      }
    }, Math.min(10 * 1000, frequency), frequency);
     historicalBroker = brokers.get(historicalCluster);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }

  @Override
  public void stop() {
    try {
    timer.cancel();
    executorService.shutdownNow();
    for (CompoundBrokerConfig brokerConfig : clusterBrokerConfig.values()) {
      if (brokerConfig.getSenseiBroker() != null) {
        brokerConfig.getSenseiBroker().shutdown();
      }
      if (brokerConfig.getNetworkClient() != null) {
      brokerConfig.getNetworkClient().shutdown();
      }
      if (brokerConfig.getClusterClient() != null) {
        brokerConfig.getClusterClient().shutdown();
      }
    }
    for (BrokerProxy brokerProxy : brokers.values()) {
      if (brokerProxy != null) {
        brokerProxy.shutdown();
      }
    }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }
  
  public void warmUp() {
    refreshTime();
  }
  public synchronized void refreshTime()   {
    try {
    SenseiBroker senseiBroker = brokers.get(historicalCluster);
    SenseiRequest req = new SenseiRequest();
    MaxMapReduce mapReduceFunction = new MaxMapReduce();
    mapReduceFunction.init(new JSONUtil.FastJSONObject().put("column", timeColumn));
    req.setMapReduceFunction(mapReduceFunction);
    req.setCount(10);
    SenseiResult result = senseiBroker.browse(req);
    JSONObject jsonRes = req.getMapReduceFunction().render(result.getMapReduceResult().getReduceResult());
    
    
    long newTimeBoundary = (long)jsonRes.getDouble("max");
    if (newTimeBoundary != timeBoundary) {
      timeBoundary = newTimeBoundary;
      currentTimeBoundaryCounter.clear();
      currentTimeBoundaryCounter.inc(timeBoundary);
      areClusterBoundariesDefined = true;      
      req = new SenseiRequest();
      BrowseSelection sel = new BrowseSelection(timeColumn);
      sel.addValue("[* TO " + newTimeBoundary + ")");
      req.addSelection(sel);
      result = senseiBroker.browse(req);
      numRelevantHistoricalEvents.clear();
      numRelevantHistoricalEvents.inc(result.getNumHitsLong());
      int numRealtimeEvents = 0;
      for (String cluster : brokers.keySet()) {
        if (cluster.equalsIgnoreCase(historicalCluster)) {
          continue;
        }
        senseiBroker = brokers.get(cluster);
        req = new SenseiRequest();
        sel = new BrowseSelection(timeColumn);
        sel.addValue("[" + newTimeBoundary + " TO *]");
        req.addSelection(sel);
        result = senseiBroker.browse(req);
        numRealtimeEvents += result.getNumHitsLong();
      }
      numRelevantRealtimeEvents.clear();
      numRelevantRealtimeEvents.inc(numRealtimeEvents);
      logger.info("Updated the time boundary between realtime and historical cluster. Now it equals to " + timeBoundary + ". There are " + numRelevantHistoricalEvents.count() + " historical events and "  + numRelevantRealtimeEvents.count() + " realtime events");
    }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    
    
  }
  
  
  public SenseiResult browse(final SenseiRequest req) throws SenseiException {
   long totalTime = System.currentTimeMillis();
   if (!areClusterBoundariesDefined) {
     refreshTime();
   }
   try {
   List<SenseiResult> senseiResults = new ArrayList<SenseiResult>();
   Future<List<SenseiResult>> historicalResults = executorService.submit(new Callable<List<SenseiResult>>() {

    @Override
    public List<SenseiResult> call() throws Exception {
      long time = System.currentTimeMillis();
      try {
      SenseiRequest cloned = req.clone();
      JSONObject historicalExpression;
      historicalExpression = new FastJSONObject().put("range", new FastJSONObject().put(timeColumn, new FastJSONObject().put("to", timeBoundary).put("include_upper", false)));
      enhanceWithTimeBoundary(cloned, historicalExpression);    
      return historicalBroker.doQuery(cloned);
      } catch (Exception ex) {
        logger.error("An exception happened when queried historical cluster", ex);
        return Collections.EMPTY_LIST;
     }finally {
        historicalClusterTime.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
      }
    }  
   });
   
   
   long time = System.currentTimeMillis();
   SenseiRequest cloned = req.clone();
   JSONObject realtimeExpression = new FastJSONObject().put("range", new FastJSONObject().put(timeColumn, new FastJSONObject().put("from", timeBoundary).put("include_lower", true)));
   enhanceWithTimeBoundary(cloned, realtimeExpression);   
   try {
   for (String cluster : brokers.keySet()) {
     if (cluster.equalsIgnoreCase(historicalCluster)) {
       continue;
     }
     BrokerProxy senseiBroker = brokers.get(cluster);
     senseiResults.addAll(senseiBroker.doQuery(cloned));   
   }  
   } catch (Exception ex) {
     logger.error("An exception happened when queried realtimeCluster", ex);
  }
   realtimeClusterTime.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS); 
   senseiResults.addAll(historicalResults.get());
   time = System.currentTimeMillis();   
   SenseiResult res = ResultMerger.merge(req, senseiResults, false); 
   mergeTime.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS); 
   logger.info("Federated request took - " + (System.currentTimeMillis() - totalTime) + " ms");
   return res;
   } catch (Exception e) {
     throw new RuntimeException(e);
   }
  }

  public void enhanceWithTimeBoundary(SenseiRequest cloned, JSONObject historicalExpression)  {
    try {
    SenseiQuery query = cloned.getQuery();
     SenseiJSONQuery senseiJSONQuery = null;
     if (query == null) {
       senseiJSONQuery = new SenseiJSONQuery(new JSONUtil.FastJSONObject().put("filter", historicalExpression));
       cloned.setQuery(senseiJSONQuery);
     } else {
       senseiJSONQuery = (SenseiJSONQuery) query;
       JSONObject jsonObject = new JSONUtil.FastJSONObject(new String(senseiJSONQuery.toBytes(), senseiJSONQuery.utf8Charset));
       if (jsonObject.opt("filter") == null) {
         jsonObject.put("filter", historicalExpression);
       } else {
         JSONObject andFilter = new JSONUtil.FastJSONObject().put("and", new JSONUtil.FastJSONArray(Arrays.asList(jsonObject.getJSONObject("filter"), historicalExpression)));
         jsonObject.put("filter", andFilter);
       }
       cloned.setQuery(new SenseiJSONQuery(jsonObject));
     }
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}