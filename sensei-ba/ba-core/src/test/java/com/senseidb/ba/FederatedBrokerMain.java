package com.senseidb.ba;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.MapConfiguration;

import com.senseidb.ba.federation.BaFederatedBroker;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class FederatedBrokerMain {
  public static void main(String[] args) throws Exception {
    SenseiPluginRegistry senseiPluginRegistry = null;
    try {
      Map<String, String> map = new HashMap<String, String>();
     
      map.put("sensei.federated.broker.class", "com.senseidb.ba.federation.BaFederatedBroker");
      map.put("sensei.federated.broker.clusters", " wvmpRealtimeEvents, wvmpOfflineEvents");
      map.put("sensei.federated.broker.historicalCluster", "wvmpOfflineEvents");
      map.put("sensei.federated.broker.timeColumn", "timeInMinutes");
      map.put("sensei.federated.broker.wvmpRealtimeEvents.sensei.cluster.url", "eat1-app184.stg.linkedin.com:10000");
      map.put("sensei.federated.broker.wvmpOfflineEvents.sensei.cluster.url", "eat1-app184.stg.linkedin.com:10000");
      MapConfiguration senseiConf = new MapConfiguration(map);
      senseiConf.setDelimiterParsingDisabled(true);
      senseiPluginRegistry = SenseiPluginRegistry.build(senseiConf);
      senseiPluginRegistry.start();
      BaFederatedBroker federatedBroker = senseiPluginRegistry.getBeanByFullPrefix("sensei.federated.broker", BaFederatedBroker.class);
      while(true) {
      SenseiRequest req = new SenseiRequest();
      req.setCount(10);
      SenseiResult result = federatedBroker.browse(req);
      System.out.println(result);
      }
    } finally {
      senseiPluginRegistry.stop();
    }
  }
}
