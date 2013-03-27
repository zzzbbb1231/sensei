package com.senseidb.ba.gazelle;

import java.util.Properties;

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
  private long _byteLength;
  private int _numberOfElements;
  private int _numberOfDictionaryValues;
  private int _bitsPerElement;
  private boolean _sorted;
  private boolean multi;
  private boolean secondarySorted;
  private String customIndexerType;
  
  public ColumnMetadata(String name, ColumnType type) {
    _name = name;
    _columnType = type;
  }

  public ColumnMetadata() {
    
  }
  public void addToConfig(Configuration configuration) {
    configuration.setProperty("column." + _name + ".numberOfElements", _numberOfElements);
    configuration.setProperty("column." + _name + ".byteLength",_byteLength);
    configuration.setProperty("column." + _name + ".numberOfDictionaryValues", _numberOfDictionaryValues);
    configuration.setProperty("column." + _name + ".bitsPerElement", _bitsPerElement);
    configuration.setProperty("column." + _name + ".sorted", _sorted);
    configuration.setProperty("column." + _name + ".columnType", _columnType);
    configuration.setProperty("column." + _name + ".multi", multi);
    configuration.setProperty("column." + _name + ".secondarySorted", secondarySorted);
    if (customIndexerType != null) {
      configuration.setProperty("column." + _name + ".customIndexerType", customIndexerType);
    }
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

public boolean isMulti() {
    return multi;
}

public void setMulti(boolean multi) {
    this.multi = multi;
}

public boolean isSecondarySorted() {
  return secondarySorted;
}

@Override
public String toString() {
  return "ColumnMetadata [_name=" + _name + ", _columnType=" + _columnType + ", _byteLength=" + _byteLength + ", _numberOfElements="
      + _numberOfElements + ", _numberOfDictionaryValues=" + _numberOfDictionaryValues + ", _bitsPerElement=" + _bitsPerElement
      + ", _sorted=" + _sorted + ", multi=" + multi +  ", secondarySorted=" + secondarySorted + "]";
}

public void setSecondarySorted(boolean secondarySorted) {
  this.secondarySorted = secondarySorted;

  
}

  public String getCustomIndexerType() {
  return customIndexerType;
}

public void setCustomIndexerType(String customIndexerType) {
  this.customIndexerType = customIndexerType;
}

  public static ColumnMetadata valueOf(Configuration config, String columnName) {
    ColumnMetadata metadata = new ColumnMetadata();
    metadata.setByteLength(config.getLong("column." + columnName + ".byteLength"));
    metadata.setNumberOfElements(config.getInt("column." + columnName + ".numberOfElements"));
    metadata.setNumberOfDictionaryValues(config.getInt("column." + columnName + ".numberOfDictionaryValues"));
    metadata.setBitsPerElement(config.getInt("column." + columnName + ".bitsPerElement"));
    metadata.setColumnType(ColumnType.valueOfStr(config.getString("column." + columnName + ".columnType")));
    metadata.setSorted(config.getBoolean("column." + columnName + ".sorted"));
    metadata.setMulti(config.getBoolean("column." + columnName + ".multi"));
    metadata.setSecondarySorted(config.getBoolean("column." + columnName + ".secondarySorted", false));
    metadata.setCustomIndexerType(config.getString("column." + columnName + ".customIndexerType", null));
    metadata.setName(columnName);
    return metadata;
  }
 
  
}
