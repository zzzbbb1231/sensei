package com.senseidb.ba.plugins;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.json.JSONObject;
import org.springframework.util.Assert;

import com.browseengine.bobo.facets.FacetHandler;
import com.linkedin.norbert.network.Serializer;
import com.senseidb.ba.realtime.indexing.IndexingCoordinator;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.indexing.ShardingStrategy;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginFactory;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.node.inmemory.MockSenseiCore;
import com.senseidb.search.plugin.PluggableSearchEngine;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;
import com.senseidb.svc.impl.SysSenseiCoreServiceImpl;

public class RealtimeSysSenseiService extends SysSenseiCoreServiceImpl implements  SenseiPlugin ,PluggableSearchEngine {
  

    public RealtimeSysSenseiService() {
    super(new MockSenseiCore(new int[0], null));
   
  }

    @Override
    public Serializer<SenseiRequest, SenseiSystemInfo> getSerializer() {
      return SysSenseiCoreServiceImpl.JAVA_SERIALIZER;
    }

    @Override
    public SenseiSystemInfo execute(SenseiRequest senseiReq) {
      senseiReq.setPartitions(partitions);
      return sysSenseiCoreServiceImpl.execute(senseiReq);
    }





  private IndexingCoordinator coordinator;
  Set<Integer> partitions = new HashSet<Integer>();
  private SysSenseiCoreServiceImpl sysSenseiCoreServiceImpl;




 
  @Override
  public void start(SenseiCore senseiCore) {
    sysSenseiCoreServiceImpl = new SysSenseiCoreServiceImpl(senseiCore);
    
  }
  @Override
  public void init(String indexDirectory, int nodeId, SenseiSchema senseiSchema, Comparator<String> versionComparator,
      SenseiPluginRegistry pluginRegistry, ShardingStrategy shardingStrategy) {
    List<IndexingCoordinator> beansByType = pluginRegistry.getBeansByType(IndexingCoordinator.class);
    Assert.state(beansByType.size() == 1);
    coordinator = beansByType.get(0);
    int numServingPartitions = coordinator.getIndexConfig().getNumServingPartitions();
    for (int i = 0; i < numServingPartitions + 2; i++) {
      partitions.add(i);
    }
  }
  @Override
  public String getVersion() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public JSONObject acceptEvent(JSONObject event, String version) {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public boolean acceptEventsForAllPartitions() {
    // TODO Auto-generated method stub
    return false;
  }
  @Override
  public Set<String> getFieldNames() {
    // TODO Auto-generated method stub
    return null;
  }
  @Override
  public Set<String> getFacetNames() {
    // TODO Auto-generated method stub
    return new HashSet<String>();
  }
  @Override
  public List<FacetHandler<?>> createFacetHandlers() {
    // TODO Auto-generated method stub
    return Collections.EMPTY_LIST;
  }
  @Override
  public void onDelete(IndexReader indexReader, long... uids) {
    // TODO Auto-generated method stub
    
  }
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    // TODO Auto-generated method stub
    
  }
  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }
  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

 

}