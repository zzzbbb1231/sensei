package com.senseidb.ba;

import org.apache.commons.configuration.Configuration;

/**
 * @author dpatel
 */

public class ColumnMetadata {
  /*
   * TODO: dpatel will keep adding things as needed.
   */
  private String _name;
  private ColumnType _columnType;
  private long _startOffset;
  private long _byteLength;
  private int _numberOfElements;
  private int _numberOfDictionaryValues;
  private int _bitsPerElement;
  private boolean _sorted;

 
  
  public ColumnMetadata(String name, ColumnType type) {
    _name = name;
    _columnType = type;
  }

  public ColumnMetadata() {
    
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

  public boolean getSorted() {
    return _sorted;
  }

  public ColumnType getColumnType() {
    return _columnType;
  }

  public void setColumnType(ColumnType originalType) {
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
