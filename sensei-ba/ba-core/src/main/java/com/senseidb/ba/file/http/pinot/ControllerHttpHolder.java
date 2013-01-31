package com.senseidb.ba.file.http.pinot;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.springframework.util.Assert;

import com.senseidb.ba.management.controller.MasterInfoServlet;
import com.senseidb.ba.management.controller.validation.ValidationServlet;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.NetUtil;

public class ControllerHttpHolder implements SenseiPlugin {

  private String directory;
  private int port;
  private Server server;
  private SenseiPluginRegistry pluginRegistry;

  private String nasBasePath;
  private Map<String, String> config;
  private String baseUrl;

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.config = config;
    this.pluginRegistry = pluginRegistry;
    String hostname = null;
    if (config.get("directory") != null) {
      setDirectoryPath(config.get("directory"));
    }
   
    Assert.notNull(directory, "directory parameter should be present");
    if (config.get("port") != null) {
      setPort(Integer.parseInt(config.get("port")));
    } else {
        throw new IllegalStateException("The Jetty port is not specified");
    }
    if (config.get("hostName") != null) {
        hostname = config.get("hostName");
      } else {
          hostname = getHostName();
      }
    
  
  
    baseUrl = "http://" + hostname + ":" + port + "/files/";
    nasBasePath = config.get("nasBasePath");
  }

  

  public void setPort(int port) {
    this.port = port;

  }
  public String getHostName() {
      try {
        return NetUtil.getHostAddress();
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
  }
  public void setDirectoryPath(String directory) {
    this.directory = directory;

  }

  @Override
  public void start() {
    server = new Server(port);
    ServletHandler servletHandler = new ServletHandler();
    servletHandler.addServletWithMapping(UploadControllerServlet.class, "/files/*");     
    for (ServletHolder holder : servletHandler.getServlets()) {
      if (holder.getHeldClass() == UploadControllerServlet.class) {
        for (String key : config.keySet()) {
         if (key.startsWith("maxPartition") || key.startsWith("zookeeper")) {
           holder.setInitParameter(key, config.get(key));
           
         }
        }
        holder.setInitParameter("directory", directory);
        holder.setInitParameter("baseUrl", baseUrl);
        holder.setInitParameter("nasBasePath", nasBasePath);
      }
    }
    server.setHandler(servletHandler);
    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public String getDirectory() {
    return directory;
  }

  public int getPort() {
    return port;
  }

  public Server getServer() {
    return server;
  }

  public void setServer(Server server) {
    this.server = server;
  }

 
  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }
}
