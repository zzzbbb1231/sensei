package com.senseidb.ba.file.http;

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
  private Map<String, Integer> clustersMaxPartitions;
  private String brokerUrl;

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    this.pluginRegistry = pluginRegistry;
    String hostname = null;
    if (config.get("directory") != null) {
      setDirectoryPath(config.get("directory"));
    }
    clustersMaxPartitions = extractClusterInfos(config);
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
    
    String maxPartitionStr = config.get("maxPartitionId");
    if (maxPartitionStr != null) {
      maxPartitionId = Integer.parseInt(maxPartitionStr);
    } else {
      maxPartitionId = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id");
    }
    brokerUrl = config.get("brokerUrl");
    if (brokerUrl == null) {
       String brokerPort = pluginRegistry.getConfiguration().getString(SenseiConfParams.SERVER_BROKER_PORT, "8080");
       
       brokerUrl = "http://localhost:" + brokerPort;
    }
    clusterName = config.get("clusterName");
    if (clusterName == null) {
      clusterName = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    }
    Assert.notNull(clusterName, "clusterName parameter should be present");
    clustersMaxPartitions.put(clusterName, maxPartitionId);
    zkUrl = config.get("zkUrl");         
    if (zkUrl == null) { 
      zkUrl = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_URL);
    }
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    baseUrl = "http://" + hostname + ":" + port + "/files/";
    nasBasePath = config.get("nasBasePath");
  }

  public static Map<String, Integer> extractClusterInfos(Map<String, String> config) {
    Map<String, Integer> ret = new HashMap<String, Integer>();
    for (String key: config.keySet()) {
      if (!key.startsWith("cluster.")) {
        continue;
      }
      String clusterName = key.substring("cluster.".length());
      String partition = config.get(key);
      ret.put(clusterName, Integer.parseInt(partition));
    }
    return ret;
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
    servletHandler.addServletWithMapping(ValidationServlet.class, "/validation/*");
    servletHandler.addServletWithMapping(AllClustersRestSegmentServlet.class, "/allsegments/*");
    servletHandler.addServletWithMapping(MasterInfoServlet.class, "/controllers/*");
    for (ServletHolder holder : servletHandler.getServlets()) {
      if (holder.getHeldClass() == FileManagementServlet.class) {
        for (String cluster : clustersMaxPartitions.keySet()) {
          holder.setInitParameter("cluster." + cluster, String.valueOf(clustersMaxPartitions.get(cluster)));
        }
        holder.setInitParameter("directory", directory);
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
      }else if (holder.getHeldClass() == ValidationServlet.class) {
        holder.setInitParameter("zkUrl", zkUrl);
        holder.setInitParameter("clusterName", clusterName);
        holder.setInitParameter("brokerUrl", brokerUrl);
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
