package com.senseidb.ba.file.http;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.springframework.util.Assert;

public class FileUploadHandler extends AbstractHandler {
  private final String directory;
  public FileUploadHandler(String directory) {
    this.directory = directory;
    // TODO Auto-generated constructor stub
  }
  @Override
  public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException,
      ServletException {
  try {
    boolean isMultipart = ServletFileUpload.isMultipartContent(request);
    if (!isMultipart) {
      return ;
    }
    DiskFileItemFactory factory = new DiskFileItemFactory();
    factory.setRepository(new File(directory));
  ServletFileUpload upload = new ServletFileUpload(factory);
  List items = upload.parseRequest(request);
  Assert.state(isMultipart, "Only multipart requests are supported");
  Iterator iter = items.iterator();
  while (iter.hasNext()) {
      FileItem item = (FileItem) iter.next();
      if (item == null || item.getName() == null) {
        continue;
      }
      System.out.println(item.getFieldName());
      
      item.write(new File(directory, item.getFieldName()));
  }
  } catch (Exception ex) {
    throw new RuntimeException(ex);
  }
  response.setStatus(200);
  ((Request) request).setHandled(true);
  }

}
