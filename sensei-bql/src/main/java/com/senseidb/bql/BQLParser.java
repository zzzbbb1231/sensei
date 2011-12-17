package com.senseidb.bql;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class BQLParser {

  private ScriptEngine engine=null;
  private final String _host;
  private final int _port;
  
  public BQLParser(String host,int port){
    _host = host;
    _port = port;
  }
  
  public void init() throws Exception{
    engine = new ScriptEngineManager().getEngineByName("python");
    engine.eval("from sensei_client import *");
    engine.put("host", _host);
    engine.put("port", _port);
    engine.eval("client = SenseiClient(host,port)");
  }
  
  public String parseBQL(String bql) throws Exception{
    engine.put("bql",bql);
    engine.eval("req = client.compile(bql)");
    engine.eval("json = client.buildJsonString(req)");
    return (String)engine.get("json");
  }
}
