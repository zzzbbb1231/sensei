package com.senseidb.ba.file.http;

import java.net.UnknownHostException;
import java.util.Map;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;

import com.senseidb.ba.management.controller.MasterInfoServlet;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.NetUtil;

public class JettyServerHolder implements SenseiPlugin {

  private String directory;
  private int port;
  private Server server;
  private SenseiPluginRegistry pluginRegistry;
  private String zkUrl;
  private String clusterName;
  private int maxPartitionId;
  private String baseUrl;
  private String nasBasePath;

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.pluginRegistry = pluginRegistry;
    String hostname = null;
    if (config.get("directory") != null) {
      setDirectoryPath(config.get("directory"));
    }
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
    maxPartitionId = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id", 0);
    zkUrl = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    clusterName = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
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

    servletHandler.addServletWithMapping(FileManagementServlet.class, "/files/*");
    servletHandler.addServletWithMapping(RestSegmentServlet.class, "/segments/*");
    servletHandler.addServletWithMapping(AllClustersRestSegmentServlet.class, "/allsegments/*");
    servletHandler.addServletWithMapping(MasterInfoServlet.class, "/controllers/*");
    for (ServletHolder holder : servletHandler.getServlets()) {
      if (holder.getHeldClass() == FileManagementServlet.class) {
        holder.setInitParameter("directory", directory);
        holder.setInitParameter("clusterName", clusterName);
        holder.setInitParameter("maxPartitionId", String.valueOf(maxPartitionId));
        holder.setInitParameter("zkUrl", zkUrl);
        holder.setInitParameter("baseUrl", baseUrl);
        holder.setInitParameter("nasBasePath", nasBasePath);
      } else if (holder.getHeldClass() == RestSegmentServlet.class) {
        holder.setInitParameter("maxPartitionId", String.valueOf(maxPartitionId));
        holder.setInitParameter("zkUrl", zkUrl);
        holder.setInitParameter("clusterName", clusterName);
      } else if (holder.getHeldClass() == AllClustersRestSegmentServlet.class) {
        holder.setInitParameter("maxPartitionId", String.valueOf(maxPartitionId));
        holder.setInitParameter("zkUrl", zkUrl);
        holder.setInitParameter("clusterName", clusterName);
      }else if (holder.getHeldClass() == MasterInfoServlet.class) {
        holder.setInitParameter("zkUrl", zkUrl);
        holder.setInitParameter("clusterName", clusterName);
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

  public String getZkUrl() {
    return zkUrl;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getMaxPartitionId() {
    return maxPartitionId;
  }

  public void setZkUrl(String zkUrl) {
    this.zkUrl = zkUrl;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setMaxPartitionId(int maxPartitionId) {
    this.maxPartitionId = maxPartitionId;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }
}
