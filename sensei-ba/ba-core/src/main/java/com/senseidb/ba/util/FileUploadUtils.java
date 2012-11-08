package com.senseidb.ba.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.PartSource;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.params.CoreProtocolPNames;

public class FileUploadUtils {
  public static void sendFile(final String url, final  String fileName, final  InputStream inputStream, final long lengthInBytes) {
    HttpClient client = new HttpClient();
    try {
    
    client.getParams().setParameter(CoreProtocolPNames.PROTOCOL_VERSION, HttpVersion.HTTP_1_1);
    PostMethod post  = new PostMethod( url );
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
  public static String getStringResponse(String urlStr) {
    try {
    URL url = new URL(urlStr);
    URLConnection con = url.openConnection();
    InputStream in = con.getInputStream();
    String encoding = con.getContentEncoding();
    encoding = encoding == null ? "UTF-8" : encoding;
    String body = IOUtils.toString(in, encoding);
    return body;
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
}
