package com.senseidb.ba.plugins;

import java.io.File;
import java.util.Map;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;
@SuppressWarnings("unchecked")
public class ZeusFactoryFactory implements ZoieFactoryFactory, SenseiPlugin {

 
  private SenseiPluginRegistry pluginRegistry;

  @Override
  public SenseiZoieFactory<?> getZoieFactory(final File idxDir, ZoieIndexableInterpreter<?> interpreter, final SenseiIndexReaderDecorator decorator,
      ZoieConfig config) {
    return new SenseiZoieFactory( idxDir, null, interpreter, decorator, config) {
      @Override
      public Zoie getZoieInstance(int nodeId, int partitionId) {
        return new ZeusIndexFactory(idxDir, new ZeusIndexReaderDecorator(pluginRegistry.resolveBeansByListKey(SenseiPluginRegistry.FACET_CONF_PREFIX, FacetHandler.class)));
      }
      @Override
      public File getPath(int nodeId, int partitionId) {
        // TODO Auto-generated method stub
        return null;
      }};
  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.pluginRegistry = pluginRegistry;
    
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
