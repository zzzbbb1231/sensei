package com.senseidb.ba.file.http;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
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
