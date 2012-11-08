package com.senseidb.ba.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.params.CoreProtocolPNames;

import com.senseidb.ba.file.http.JettyServerHolder;

public class FileUploadUtils {
  public static void sendFile(final String host, final String port, final  String fileName, final  InputStream inputStream, final long lengthInBytes) {
    HttpClient client = new HttpClient();
    try {
    
    client.getParams().setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
    PostMethod post  = new PostMethod( "http://" + host + ":" + port + "/files/" );
    Part[] parts = {       
        new FilePart(fileName, new PartSource() {          
          @Override
          public long getLength() {
            return lengthInBytes;
          }
          
          @Override
          public String getFileName() {
            return "fileName";
          }
          @Override
          public InputStream createInputStream() throws IOException {
            return new BufferedInputStream(inputStream);
          }
        })
    };
    post.setRequestEntity(new MultipartRequestEntity(parts, new HttpMethodParams()));
   
      client.executeMethod(post);
    } catch (Exception e) {
     throw new RuntimeException(e);
    } finally {      
     
    }
  }
  public static String listFiles(String host, String port) {
    try {
    HttpClient httpClient = new HttpClient();
    GetMethod httpget = new GetMethod("http://" + host + ":" + port + "/files/");
    httpClient.executeMethod(httpget);
    return IOUtils.toString(httpget.getResponseBodyAsStream());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  public static long  getFile(String host, String port, String remoteFileName, File file) {
    try {
      HttpClient httpClient = new HttpClient();
      GetMethod httpget = new GetMethod("http://" + host + ":" + port + "/files/" + remoteFileName);
      httpClient.executeMethod(httpget);
      long ret = httpget.getResponseContentLength();
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file));
      IOUtils.copyLarge(httpget.getResponseBodyAsStream(), output);
      IOUtils.closeQuietly(output);
  
      return ret;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
}
  public static void main(String[] args) throws Exception {
    JettyServerHolder jettyServerHolder = new JettyServerHolder();
    jettyServerHolder.setPort(8088);
    String directory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(directory));
    new File(directory).mkdirs();
    jettyServerHolder.setDirectoryPath(directory);
    jettyServerHolder.start();
    FileUploadUtils.sendFile("localhost","8088",  "workspace.tar.gz", new FileInputStream("/tmp/ba-index-standalone/exploded/workspace.tar.gz"), new File("/tmp/ba-index-standalone/exploded/workspace.tar.gz").length());
    String stringResponse = FileUploadUtils.listFiles("localhost","8088");
    System.out.println(stringResponse);
  }
}
