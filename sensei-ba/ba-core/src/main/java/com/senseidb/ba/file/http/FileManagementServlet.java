package com.senseidb.ba.file.http;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.ba.util.TarGzCompressionUtils;
import com.senseidb.indexing.activity.time.Clock;
import com.senseidb.util.NetUtil;

public class FileManagementServlet extends HttpServlet {
  private static final int _10_MINUTES = 10 * 60 * 1000;
  private static Logger logger = Logger.getLogger(FileManagementServlet.class);  
  private String directory = "/tmp/uploads";
  private String baseUrl;
  private String defaultClusterName;
  //TODO
  private Map<String, Integer> clustersMaxPartitions = new HashMap<String, Integer>();
  private ZkManager zkManager;

  private String nasBasePath;
  @Override
  public void init(ServletConfig config) throws ServletException {
    directory = config.getInitParameter("directory");
    Assert.notNull(directory, "directory parameter should be present");
   
    File dir = new File(directory);
    if (!dir.exists()) {
      dir.mkdirs();
    }
   
     nasBasePath = config.getInitParameter("nasBasePath");
     clustersMaxPartitions  = extractClusterPartitions(config);
    
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
    if (nasBasePath != null && !nasBasePath.endsWith("/")) {
      nasBasePath += "/";
    }
    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }
    String zkUrl = config.getInitParameter("zkUrl");
    Assert.notNull(zkUrl, "zkUrl parameter should be present");
    if (clustersMaxPartitions.size() == 1) {
      defaultClusterName = clustersMaxPartitions.keySet().iterator().next();
    }
    zkManager = new ZkManager(zkUrl, defaultClusterName);
    super.init(config);
  }

  private Map<String, Integer> extractClusterPartitions(ServletConfig config) {
    Map<String, Integer>  ret = new HashMap<String, Integer>();
    Enumeration initParameterNames = config.getInitParameterNames();
    while (initParameterNames.hasMoreElements()) {
      String key = (String) initParameterNames.nextElement();
      if (!key.startsWith("cluster.")) {
        continue;
      }
      String clusterName = key.substring("cluster.".length());
      String partition = config.getInitParameter(key);
      ret.put(clusterName, Integer.parseInt(partition));
    }
    return ret;
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
      CheckedOutputStream checkedOutputStream = new CheckedOutputStream(outputStream, new CRC32());
      try {
        IOUtils.copyLarge(req.getInputStream(), checkedOutputStream);        
        resp.setStatus(HttpServletResponse.SC_CREATED);     
      } finally {
        checkedOutputStream.close();
      }
      notifyZookeeperNewFileCreated(file, checkedOutputStream.getChecksum().getValue()); 
    }
  }

  private void notifyZookeeperNewFileCreated(File file, long checksum) {
    try {
   
      InputStream metadata = TarGzCompressionUtils.unTarOneFile(new FileInputStream(file), GazelleUtils.METADATA_FILENAME);
      PropertiesConfiguration properties = new PropertiesConfiguration();
      properties.setDelimiterParsingDisabled(true);
      properties.load(metadata);
      IOUtils.closeQuietly(metadata);
      Map<String, String> segmentMetadata = SegmentPersistentManager.getSegmentMetadata(properties);
      
      if (!segmentMetadata.containsKey("timeCreated")) {
        segmentMetadata.put("timeCreated", "" + System.currentTimeMillis());
      }
      String clusterName = this.defaultClusterName;
      if (segmentMetadata.containsKey("segment.cluster.name")) {
        clusterName = segmentMetadata.get("segment.cluster.name");
        logger.info("The deployment cluster name is overriden to - " + clusterName);
      }
      if (checksum == -1 && segmentMetadata.get(SegmentMetadata.SEGMENT_CRC) == null) {
        extractCheckSum(file, segmentMetadata);
      } else if (checksum != -1) {
        segmentMetadata.put(SegmentMetadata.SEGMENT_CRC, String.valueOf(checksum));
      }
      Assert.state(clustersMaxPartitions.containsKey(clusterName), "The configuration doesn't contain the maxPartitionId entry for the cluster " + clusterName);
      int partitionId = getPartition(file.getName(), clustersMaxPartitions.get(clusterName));
      if (nasBasePath != null) {
        String path = nasBasePath + clusterName;
        File nasDir = new File(path);
        if (!nasDir.exists()) {
          nasDir.mkdirs();
        }        
        File nasFile = new File(nasDir, file.getName());
        if (!nasFile.exists() || nasFile.lastModified() < Clock.getTime() - _10_MINUTES || nasFile.length() != file.length()) {
          Assert.state(file.renameTo(nasFile), "Couldn't rename file to " + nasFile.getAbsolutePath());
          logger.info("Registering the segment  " + nasFile.getAbsolutePath() + " in nas");
          zkManager.registerSegment(clusterName, partitionId, file.getName(), nasFile.getAbsolutePath(), segmentMetadata);
        } else {
          logger.info("Ignoring the segment  " + file.getName() + " as its already there");
        }
       
      } else {
        zkManager.registerSegment(partitionId, file.getName(), baseUrl + file.getName(), segmentMetadata);
        logger.info("Registering the segment  " + (baseUrl + file.getName()) + " in nas");
      }
       
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      throw new RuntimeException(ex);
    }
    
  }

  public void extractCheckSum(File file, Map<String, String> segmentMetadata) throws FileNotFoundException, IOException {
    long checksum;
    FileInputStream fis = null;
    try {    
    fis =    new FileInputStream(file);  
    CRC32 crc = new CRC32();
    CheckedInputStream cis = new CheckedInputStream(fis, crc);
    byte[] buffer = new byte[4096];
    while(cis.read(buffer)>=0){}
    checksum = cis.getChecksum().getValue();
    segmentMetadata.put(SegmentMetadata.SEGMENT_CRC, String.valueOf(checksum));
    
   } finally {
      fis.close();
    }
  }

  public static int getPartition(String segmentId, int maxPartition) {
    int partitionId = Math.abs(segmentId.hashCode()) % (maxPartition + 1);
    return partitionId;
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
        notifyZookeeperNewFileCreated(file, -1);
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
