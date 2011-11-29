package com.senseidb.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

public class SenseiStatement implements Statement {

  private final SenseiConnection _conn;
  public SenseiStatement(SenseiConnection conn){
    _conn = conn;
  }
  
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void addBatch(String arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cancel() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean execute(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0, int[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String arg0, String[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet executeQuery(String query) throws SQLException {
    return new SenseiResultSet(this);
  }

  @Override
  public int executeUpdate(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0, int arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0, int[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String arg0, String[] arg1) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return _conn;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxRows() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean getMoreResults(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setCursorName(String arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setEscapeProcessing(boolean arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setFetchDirection(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setFetchSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setMaxFieldSize(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setMaxRows(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setPoolable(boolean arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setQueryTimeout(int arg0) throws SQLException {
    // TODO Auto-generated method stub

  }

}
