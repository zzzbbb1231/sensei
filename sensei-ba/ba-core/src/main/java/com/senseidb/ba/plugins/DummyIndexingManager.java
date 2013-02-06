package com.senseidb.ba.plugins;

import java.util.Map;

import org.json.JSONObject;

import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.node.SenseiIndexingManager;

public class DummyIndexingManager implements SenseiIndexingManager<JSONObject> {

  @Override
  public void initialize(Map<Integer, Zoie<BoboIndexReader, JSONObject>> zoieSystemMap) throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void start() throws Exception {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DataProvider<JSONObject> getDataProvider() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void syncWithVersion(long timeToWait, String version) throws ZoieException {
    // TODO Auto-generated method stub
    
  }

}
