package com.senseidb.ba.file.http;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

public class FileListHandler extends AbstractHandler {
  private final String directory;
  public FileListHandler(String directory) {
    this.directory = directory;
  }
  @Override
  public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException,
      ServletException {
    if( "/list/".equals(request.getPathInfo()) ||"/list".equals(request.getPathInfo())) {
      for (File file : new File(directory).listFiles()) {
        if (file.isDirectory()) {
          continue;
        }
        response.getOutputStream().println(file.getName());
      }
      response.getOutputStream().close();
      response.setStatus(200);
      ((Request) request).setHandled(true);
    }
    
  }
  

}
