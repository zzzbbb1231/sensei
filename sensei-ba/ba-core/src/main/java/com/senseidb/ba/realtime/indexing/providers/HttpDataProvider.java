package com.senseidb.ba.realtime.indexing.providers;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.component.LifeCycle.Listener;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.springframework.util.Assert;

import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.JSONUtil;

public class HttpDataProvider implements RealtimeDataProvider,SenseiPlugin {
  private int port;
  private Server server;
  private int capacity = 5000;
  private ArrayBlockingQueue<Object[]> blockingQueue;
  private Schema schema;
  private int counter = 0;
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    
    String port = config.get("port");
    Assert.notNull(port, "port is not defined for HttpDataProvider");
    this.port = Integer.parseInt(port);
    String capacityStr = config.get("capacity");
    if (capacityStr != null) {
      capacity = Integer.parseInt(capacityStr);
    }
  }

  @Override
  public void init(Schema schema, String lastVersion) {
    this.schema = schema;
    
  }

  @Override
  public void start() {
    if (blockingQueue != null) return;
     blockingQueue = new ArrayBlockingQueue<Object[]>(capacity); 
    server = new Server(port);
     server.setHandler(new AbstractHandler() {
      
      @Override
      public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException,
          ServletException {
        ServletInputStream inputStream = request.getInputStream();
        try {
        String str = IOUtils.toString(inputStream);
        str = str.trim();
        if (str.startsWith("[")) {
          JSONArray jsonArray = new JSONUtil.FastJSONArray(str);
          for (int i = 0; i < jsonArray.length(); i++) {
            blockingQueue.put(schema.fromJson(jsonArray.getJSONObject(i)));
          }
        } else {
          blockingQueue.add(schema.fromJson(new JSONUtil.FastJSONObject(str)));
        }
        } catch (Exception ex) {
          throw new RuntimeException(ex.getMessage(), ex);
        }finally {
          IOUtils.closeQuietly(inputStream);
        }
      }
    });
     try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    blockingQueue.clear();
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
  }

  @Override
  public DataWithVersion next() {
    
    final Object[] poll = blockingQueue.poll();
    if (poll == null) {
      return null;
    }
    return new DataWithVersion() {
      
      @Override
      public String getVersion() {
        return "" + counter++;
      }
      
      @Override
      public Object[] getValues() {
        return poll;
      }
    };
  }

  @Override
  public void commit(String version) {
    // TODO Auto-generated method stub
    
  }

}
