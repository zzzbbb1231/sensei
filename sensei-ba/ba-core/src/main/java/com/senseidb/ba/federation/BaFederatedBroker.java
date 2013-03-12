package com.senseidb.ba.federation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.api.BrowseSelection;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;
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

public class BaFederatedBroker extends LayeredBroker {
  private static Logger logger = Logger.getLogger(BaFederatedBroker.class);
  private static final Counter numRelevantHistoricalEvents = Metrics.newCounter(BaFederatedBroker.class, "numRelevantHistoricalEvents");
  private static final Counter numRelevantRealtimeEvents = Metrics.newCounter(BaFederatedBroker.class, "numfRelevantRealtimeEvents");
  private static Timer timer = new Timer(true);
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
  
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
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
   
  }
  
  @Override
  public void start() {
    for (String cluster : clusters) {
      CompoundBrokerConfig brokerConfig = clusterBrokerConfig.get(cluster);
      brokerConfig.init();
      brokers.put(cluster, new BrokerProxy(brokerConfig.getNetworkClient(), brokerConfig.getClusterClient(), true));
    }
    timer.schedule(new TimerTask() {
      
      @Override
      public void run() {
       
          refreshTime();
       
        
      }
    }, Math.min(10 * 1000, frequency), frequency);
  }

  @Override
  public void stop() {
    for (CompoundBrokerConfig brokerConfig : clusterBrokerConfig.values()) {
      brokerConfig.getSenseiBroker().shutdown();
      brokerConfig.getNetworkClient().shutdown();
      brokerConfig.getClusterClient().shutdown();
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
   if (!areClusterBoundariesDefined) {
     refreshTime();
   }
   try {
   SenseiRequest cloned = req.clone();
   BrokerProxy historicalBroker = brokers.get(historicalCluster);
   List<SenseiResult> senseiResults = new ArrayList<SenseiResult>();
   JSONObject historicalExpression;
    historicalExpression = new FastJSONObject().put("range", new FastJSONObject().put(timeColumn, new FastJSONObject().put("to", timeBoundary).put("include_upper", false)));
   
   enhanceWithTimeBoundary(cloned, historicalExpression);
   senseiResults.addAll(historicalBroker.doQuery(cloned));
   cloned = req.clone();
   JSONObject realtimeExpression = new FastJSONObject().put("range", new FastJSONObject().put(timeColumn, new FastJSONObject().put("from", timeBoundary).put("include_lower", true)));
   enhanceWithTimeBoundary(cloned, realtimeExpression);   
   for (String cluster : brokers.keySet()) {
     if (cluster.equalsIgnoreCase(historicalCluster)) {
       continue;
     }
     BrokerProxy senseiBroker = brokers.get(cluster);
     senseiResults.addAll(senseiBroker.doQuery(cloned));   
   }   
    SenseiResult res = ResultMerger.merge(req, senseiResults, false); 
    return res;
   } catch (JSONException e) {
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