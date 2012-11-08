package com.senseidb.ba.file.http;

import java.util.Map;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;

public class JettyServerHolder implements SenseiPlugin {

  private String directory;
  private int port;
  private Server server;

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    if (config.get("directory") != null) {
      setDirectoryPath(config.get("directory"));
    }
    if (config.get("port") != null) {
      setPort(Integer.parseInt(config.get("port")));
    } else {
      throw new IllegalStateException("The Jetty port is not specified");
    }
    
  }
  public void setPort(int port) {
    this.port = port;
    
  }
  public void setDirectoryPath(String directory) {
    this.directory = directory;
    
  }
  @Override
  public void start() {
     server = new Server(port);
     ServletHandler servletHandler = new ServletHandler();
     
     servletHandler.addServletWithMapping(FileManagementServlet.class, "/files/*");
     servletHandler.getServlets()[0].setInitParameter("directory", directory);
     
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
   
}
