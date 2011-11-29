package com.senseidb.jdbc.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCClient {

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    Class.forName("com.senseidb.jdbc.SenseiDriver");
    String url = "jdbc:sensei://localhost:8080";
    
    Connection conn = DriverManager.getConnection(url,"username",null);
    
    String query = "select color,category from cars";
    Statement st = conn.createStatement();
    ResultSet rs = st.executeQuery(query);
    while (rs.next())
    {
      String s = rs.getString("color");
      System.out.println(s);
    }
    conn.close();
  }

}
