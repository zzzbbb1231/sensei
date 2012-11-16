package com.senseidb.ba.file.http;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.springframework.util.Assert;

import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.util.NetUtil;

public class FileManagementServlet extends HttpServlet {
  private static Logger logger = Logger.getLogger(FileManagementServlet.class);  
  private String directory = "/tmp/uploads";
  private String baseUrl;
  private String clusterName;
  private ZkManager zkManager;
  private int maxPartition;
  
  @Override
  public void init(ServletConfig config) throws ServletException {
    directory = config.getInitParameter("directory");
    Assert.notNull(directory, "directory parameter should be present");
    File dir = new File(directory);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    baseUrl = config.getInitParameter("baseUrl");
    if (baseUrl == null) {
      String port = config.getInitParameter("port");
      Assert.notNull(port, "Either baseUrl or port parameter should be present");
      try {
        baseUrl = "http://" + NetUtil.getHostAddress() + ":" + port + "/files/";
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    clusterName = config.getInitParameter("clusterName");
    Assert.notNull(clusterName, "clusterName parameter should be present");
    zkManager = new ZkManager(zkUrl, clusterName);
    String maxPartitionId = config.getInitParameter("maxPartitionId");
    Assert.notNull(maxPartition, "maxPartition parameter should be present");
    maxPartition = Integer.parseInt(maxPartitionId);
    super.init(config);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String fileName = getFileName(req);
    if(fileName == null || "".equals(fileName) || !(new File(directory, fileName).exists())) {
      printDirectoryList(resp);
      resp.setStatus(HttpServletResponse.SC_OK);
      return;
    }
    File file = new File(directory, fileName);
    resp.setContentType("application/octet-stream");
    resp.setContentLength((int)file.length());
    BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
    IOUtils.copyLarge(input, resp.getOutputStream());
    resp.getOutputStream().flush();
    resp.getOutputStream().close();
    resp.setStatus(200);
  }
  public void printDirectoryList(HttpServletResponse response) throws IOException {
    JSONArray array = new JSONArray();
    for (File file : new File(directory).listFiles()) {
      if (file.isDirectory()) {
        continue;
      }
      array.put(file.getName());
    }
    response.getOutputStream().print(array.toString());
    response.getOutputStream().flush();
    response.getOutputStream().close();
    response.setStatus(200);
  }
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    boolean isMultipart = ServletFileUpload.isMultipartContent(req);
    if (isMultipart) {
      logger.info("Received new MultiPartPost request");
      handleMultiPartUpload(req, resp);      
    } else {
      String fileName = getFileName(req);
      if(fileName == null) {
        resp.setStatus(HttpServletResponse.SC_MULTIPLE_CHOICES);        
        return;
      }
      File file = new File(directory, fileName);
      BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
      try {
        IOUtils.copyLarge(req.getInputStream(), outputStream);        
        resp.setStatus(HttpServletResponse.SC_CREATED);     
      } finally {
        outputStream.close();
      }
      notifyZookeeperNewFileCreated(file); 
    }
  }

  private void notifyZookeeperNewFileCreated(File file) {
    int partitionId = Math.abs(file.getName().hashCode()) % (maxPartition + 1);
    SegmentType segmentType =  SegmentType.COMPRESSED_GAZELLE;
    zkManager.registerSegment(partitionId, file.getName(), baseUrl + file.getName(), segmentType, System.currentTimeMillis(), -1L);
  }

  public void handleMultiPartUpload(HttpServletRequest req, HttpServletResponse resp) {
    try {
      DiskFileItemFactory factory = new DiskFileItemFactory();
      factory.setRepository(new File(directory));
      ServletFileUpload upload = new ServletFileUpload(factory);
      List items = upload.parseRequest(req);
      Iterator iter = items.iterator();
      while (iter.hasNext()) {
        FileItem item = (FileItem) iter.next();
        if (item == null || item.getName() == null) {
          continue;
        }

        File file = new File(directory, item.getFieldName());
        if (file.exists()) {
          logger.warn("The file " + file.getAbsolutePath() + " already exists. Replcaing it with the new one");
          FileUtils.deleteQuietly(file);
        }
        item.write(file);
        notifyZookeeperNewFileCreated(file);
        logger.info("Finished uploading file - " + item.getFieldName());
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
   
    resp.setStatus(HttpServletResponse.SC_CREATED);     
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String fileName = getFileName(req);
    if(fileName == null) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    File file = new File(directory, fileName);
    if (!file.exists()) {
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    FileUtils.deleteQuietly(file);
    logger.info("Deleted file - " + file.getName());
    resp.setStatus(HttpServletResponse.SC_OK);
  }
  public String getFileName(HttpServletRequest httpServletRequest) {
    if (httpServletRequest.getPathInfo() == null) {
      return null;
    }
    String fileName = null;
    if (httpServletRequest.getPathInfo().contains("/")) {
      fileName = httpServletRequest.getPathInfo().substring(httpServletRequest.getPathInfo().lastIndexOf("/") + 1);
      if (fileName.contains("&")) {
        throw new IllegalArgumentException("Servlet doesn't support parameters delimited with &");
      }      
    }
    return fileName;
  }
  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    try {
      super.service(req, resp);
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      try {
        ex.printStackTrace(new PrintWriter(resp.getOutputStream()));
        resp.getOutputStream().flush();
        resp.getOutputStream().close();
        resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }
  }
 

}
