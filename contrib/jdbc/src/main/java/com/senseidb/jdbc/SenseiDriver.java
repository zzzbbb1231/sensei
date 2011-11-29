package com.senseidb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class SenseiDriver implements Driver {

  private static String PREFIX = "jdbc:sensei://";
  static{
    try
    {
    // Register the BEDriver with DriverManager
      SenseiDriver driverInst = new SenseiDriver();
      DriverManager.registerDriver(driverInst);
    }
    catch(Exception e){
      throw new RuntimeException(e.getMessage(),e);
    }
  }
  
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(PREFIX);
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    String hostPortString = url.substring(PREFIX.length());
    System.out.println("connect: "+hostPortString);
    String[] hostPortPair = hostPortString.split(":");
    
    if (hostPortPair.length<1){
      throw new SQLException("unable to extract host and port from url: "+url);
    }
    
    String host = hostPortPair[0];

    int port = 8080;
    if (hostPortPair.length>1){
      port = Integer.parseInt(hostPortPair[1]);
    }
    
    System.out.println("host: "+host+", port: "+port);
    
    String user = props.getProperty("user");
    String pw = props.getProperty("password");
    return new SenseiConnection(host,port);
  }

  @Override
  public int getMajorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMinorVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean jdbcCompliant() {
    // TODO Auto-generated method stub
    return false;
  }

}
