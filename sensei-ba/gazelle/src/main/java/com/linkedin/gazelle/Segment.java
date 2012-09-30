package com.linkedin.gazelle;

import java.util.HashMap;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;

public class Segment {
  private HashMap<String, ColumnMedata> _columnMetatdaMap;
  private HashMap<String, TermValueList> _termValueListMap;
  private HashMap<String, CompressedIntArray> _compressedIntArrayMap;
  private int _length;

  public Segment(HashMap<String, ColumnMedata> metadataMap, HashMap<String, TermValueList> termValueListMap, HashMap<String, CompressedIntArray> compressedIntArrayMap, int length) {
    _columnMetatdaMap = metadataMap;
    _termValueListMap = termValueListMap;
    _compressedIntArrayMap = compressedIntArrayMap;
    _length = length;
  }

  public Segment() {

  }

  public int getLength() {
    return _length;
  }

  public void setLength(int length) {
    this._length = length;
  }

  public HashMap<String, ColumnMedata> getColumnMetatdaMap() {
    return _columnMetatdaMap;
  }

  public void setColumnMetatdaMap(HashMap<String, ColumnMedata> columnMetatdaMap) {
    this._columnMetatdaMap = columnMetatdaMap;
  }

  public HashMap<String, TermValueList> getTermValueListMap() {
    return _termValueListMap;
  }

  public void setTermValueListMap(HashMap<String, TermValueList> termValueListMap) {
    this._termValueListMap = termValueListMap;
  }

  public HashMap<String, CompressedIntArray> getForwardIndexMap() {
    return _compressedIntArrayMap;
  }

  public void setForwardIndexMap(HashMap<String, CompressedIntArray> forwardIndexMap) {
    this._compressedIntArrayMap = forwardIndexMap;
  }

}
