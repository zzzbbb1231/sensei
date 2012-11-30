package com.senseidb.ba.file.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.ba.AvroConverter;
import com.senseidb.ba.util.FileUploadUtils;

public class FileManagementServletTest {
  private JettyServerHolder jettyServerHolder;
  private String directory;
  @Before
  public void setUp() throws Exception {
    jettyServerHolder = new JettyServerHolder();
    jettyServerHolder.setPort(8088);
     directory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(directory));
    new File(directory).mkdirs();
    jettyServerHolder.setDirectoryPath(directory);
    jettyServerHolder.setClusterName("bla");
    jettyServerHolder.setMaxPartitionId(0);
    jettyServerHolder.setZkUrl("localhost:2181");
    jettyServerHolder.setBaseUrl("http://localhost:8088/files/");
    jettyServerHolder.start();
  }
  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(directory));
    jettyServerHolder.stop();
  }
  @Test
  public void test1() throws Exception {
    File avroFile = new File(AvroConverter.class.getClassLoader().getResource("data/sample_data.avro").toURI());
      FileUploadUtils.sendFile("localhost","8088", avroFile.getName(), new FileInputStream(avroFile), avroFile.length());
    String stringResponse = FileUploadUtils.listFiles("localhost", "8088");
    System.out.println(stringResponse);
    assertTrue(stringResponse.contains("sample_data.avro"));
    assertEquals(avroFile.length(), FileUploadUtils.getFile("localhost", "8088", "sample_data.avro", new File("sample_data.avro1")));
  }
 

}
