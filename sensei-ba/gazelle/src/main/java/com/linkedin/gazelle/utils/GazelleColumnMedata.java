package com.linkedin.gazelle.utils;

import it.unimi.dsi.fastutil.Hash;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * @author dpatel
 */

public class GazelleColumnMedata {
  /*
   * TODO: dpatel will keep adding things as needed.
   */
  private String _name;
  private GazelleColumnType _columnType;
  private long _startOffset;
  private long _byteLength;
  private int _numberOfElements;
  private int _numberOfDictionaryValues;
  private int _bitsPerElement;
  private boolean _sorted;

  public static HashMap<String, GazelleColumnMedata> readFromFile(PropertiesConfiguration config) {
    HashMap<String, GazelleColumnMedata> columnMetadataMap = new HashMap<String, GazelleColumnMedata>();
    Iterator columns = config.getKeys("column");
    while (columns.hasNext()) {
       String key = (String) columns.next();
       String columnName = key.split("\\.")[1];
       GazelleColumnMedata metadata = new GazelleColumnMedata();
       metadata.setStartOffset(config.getLong("column." + columnName + ".startOffset"));
       metadata.setByteLength(config.getLong("column." + columnName + ".byteLength"));
       metadata.setNumberOfElements(config.getInt("column." + columnName + ".numberOfElements"));
       metadata.setNumberOfDictionaryValues(config.getInt("column." + columnName + ".numberOfDictionaryValues"));
       metadata.setBitsPerElement(config.getInt("column." + columnName + ".bitsPerElement"));
       metadata.setColumnType(GazelleColumnType.getType(config.getString("column." + columnName + ".columnType")));
       metadata.setSorted(config.getBoolean("column." + columnName + ".sorted"));
       metadata.setName(columnName);
       if (!columnMetadataMap.containsKey(columnName)) {
         columnMetadataMap.put(columnName, metadata);
       }
    }
    return columnMetadataMap;
  }
  
  public GazelleColumnMedata(String name, GazelleColumnType type) {
    _name = name;
    _columnType = type;
  }

  public GazelleColumnMedata() {
    
  }
  public void addToConfig(Configuration configuration) {
    configuration.setProperty("column." + _name + ".startOffset", _startOffset);
    configuration.setProperty("column." + _name + ".numberOfElements", _numberOfElements);
    configuration.setProperty("column." + _name + ".byteLength",_byteLength);
    configuration.setProperty("column." + _name + ".numberOfDictionaryValues", _numberOfDictionaryValues);
    configuration.setProperty("column." + _name + ".bitsPerElement", _bitsPerElement);
    configuration.setProperty("column." + _name + ".sorted", _sorted);
    configuration.setProperty("column." + _name + ".columnType", _columnType.toString());
  }

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    this._name = name;
  }

  public GazelleColumnType getColumnType() {
    return _columnType;
  }

  public void setColumnType(GazelleColumnType originalType) {
    this._columnType = originalType;
  }

  public long getStartOffset() {
    return _startOffset;
  }

  public void setStartOffset(long startOffset) {
    this._startOffset = startOffset;
  }

  public long getByteLength() {
    return _byteLength;
  }

  public void setByteLength(long byteLength) {
    this._byteLength = byteLength;
  }

  public int getNumberOfElements() {
    return _numberOfElements;
  }

  public void setNumberOfElements(int numberOfElements) {
    this._numberOfElements = numberOfElements;
  }

  public int getNumberOfDictionaryValues() {
    return _numberOfDictionaryValues;
  }

  public void setNumberOfDictionaryValues(int numberOfDictionaryValues) {
    this._numberOfDictionaryValues = numberOfDictionaryValues;
  }

  public int getBitsPerElement() {
    return _bitsPerElement;
  }

  public void setBitsPerElement(int bitsPerElement) {
    this._bitsPerElement = bitsPerElement;
  }

  public boolean isSorted() {
    return _sorted;
  }

  public void setSorted(boolean sorted) {
    this._sorted = sorted;
  }

}
