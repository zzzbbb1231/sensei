package com.senseidb.ba.file.http;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.springframework.util.Assert;

public class FileDownloadHandler extends AbstractHandler {
  private final String directory;
  public FileDownloadHandler(String directory) {
    this.directory = directory;
  }
  @Override
  public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException,
      ServletException {
    
    if( (request.getPathInfo() != null && request.getPathInfo().startsWith("/files/"))) {
      String fileName = request.getPathInfo().substring("/files/".length());
      File file = new File(directory, fileName);
      Assert.state(file.exists(), " file doesn't exist");
      response.setContentType("application/octet-stream");
      response.setContentLength((int)file.length());
      BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
      IOUtils.copyLarge(input, response.getOutputStream());
      response.getOutputStream().flush();
      response.getOutputStream().close();
      response.setStatus(200);
      ((Request) request).setHandled(true);
    }
  }
}
