package com.senseidb.ba.file.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.ba.AvroConverter;
import com.senseidb.ba.file.http.pinot.ControllerHttpHolder;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;

public class ControllerManagementServletTest {
  private ControllerHttpHolder jettyServerHolder;
  private String directory;
  private File tmpDir;
  @Before
  public void setUp() throws Exception {
    jettyServerHolder = new ControllerHttpHolder();
    jettyServerHolder.setPort(8088);
     directory = "/tmp/fileUpload";
     tmpDir = new File("tmp");
     FileUtils.deleteDirectory(tmpDir);
     tmpDir.mkdirs();
     FileUtils.deleteDirectory(new File(directory));
    
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/bla1");
    zkClient.deleteRecursive("/sensei-ba/bla2");
    new File(directory).mkdirs();
    Map<String, String> config = new HashMap<String, String>();
    config.put("directory", directory);
    config.put("nasBasePath", directory + "/nas");
    config.put("maxPartition.bla1", "0");   
    config.put("zookeeper.bla1", "localhost:2181");
    config.put("maxPartition.bla2", "1");   
    config.put("zookeeper.bla2", "localhost:2181");
    config.put("port", "8088");  
    jettyServerHolder.init(config, null);  
    jettyServerHolder.start();
  }
  @After
  public void tearDown() throws Exception {
    tmpDir = new File("tmp");
    FileUtils.deleteDirectory(tmpDir);
    FileUtils.deleteDirectory(new File(directory));
    jettyServerHolder.stop();
  }
  @Test
  public void test1() throws Exception {
    GazelleIndexSegmentImpl indexSegmentImpl = TestUtil.createIndexSegment();
    indexSegmentImpl.getSegmentMetadata().put(SegmentMetadata.SEGMENT_CLUSTER_NAME, "bla1");
    File compressedFile = TestUtil.createCompressedSegment("segmentForBla1", indexSegmentImpl, tmpDir);
    FileUploadUtils.sendFile("localhost","8088", "segmentForBla1", new FileInputStream(compressedFile), compressedFile.length());
   indexSegmentImpl.getSegmentMetadata().put(SegmentMetadata.SEGMENT_CLUSTER_NAME, "bla2");
    File compressedFile2 = TestUtil.createCompressedSegment("segmentForBla2", indexSegmentImpl, tmpDir);
    FileUploadUtils.sendFile("localhost","8088", "segmentForBla2", new FileInputStream(compressedFile2), compressedFile2.length());
   
    Thread.sleep(500);
    String stringResponse = FileUploadUtils.listFiles("localhost", "8088");
    assertEquals("[\"bla1/segmentForBla1\",\"bla2/segmentForBla2\"]", stringResponse);
   
    assertEquals(compressedFile2.length(), FileUploadUtils.getFile("localhost", "8088", "bla2/segmentForBla2", new File(tmpDir, "sample_data.avro1")));
  }
 

}
