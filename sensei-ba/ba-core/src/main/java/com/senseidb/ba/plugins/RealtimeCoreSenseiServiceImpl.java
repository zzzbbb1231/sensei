package com.senseidb.ba.plugins;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.util.Assert;

import com.linkedin.norbert.network.Serializer;
import com.senseidb.ba.realtime.indexing.IndexingCoordinator;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiCore;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.svc.impl.CoreSenseiServiceImpl;

public class RealtimeCoreSenseiServiceImpl extends CoreSenseiServiceImpl implements SenseiPlugin {
  private IndexingCoordinator coordinator;
  Set<Integer> partitions = new HashSet<Integer>();
  public RealtimeCoreSenseiServiceImpl(SenseiCore core) {
    super(core);    
  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    List<IndexingCoordinator> beansByType = pluginRegistry.getBeansByType(IndexingCoordinator.class);
    Assert.state(beansByType.size() == 1);
    coordinator = beansByType.get(0);
    
  }
@Override
public SenseiResult execute(SenseiRequest senseiReq) {
  senseiReq.setPartitions(partitions);
  return super.execute(senseiReq);
}
  @Override
  public void start() {
    int numServingPartitions = coordinator.getIndexConfig().getNumServingPartitions();
    for (int i = 0; i < numServingPartitions + 2; i++) {
      partitions.add(i);
    }
  }
@Override
public Serializer<SenseiRequest, SenseiResult> getSerializer() {
  return CoreSenseiServiceImpl.JAVA_SERIALIZER;
}
  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

}
