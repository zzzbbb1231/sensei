package com.senseidb.ba.gazelle.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.utils.SortUtil;

public class SecondarySortedForwardIndexImpl implements SingleValueForwardIndex, SecondarySortedForwardIndex , MetadataAware {
  private TermValueList<?> dictionary;
  private int length;
  private ColumnMetadata columnMetadata;
  private SortedRegion[] sortedRegions;
  public SecondarySortedForwardIndexImpl(TermValueList<?> dictionary, SortedRegion[] sortedRegions, int length, ColumnMetadata columnMetadata) {
      super();
      this.dictionary = dictionary;
      this.sortedRegions = sortedRegions;      
      this.length = length;
      this.columnMetadata = columnMetadata;     
  }
  public SecondarySortedForwardIndexImpl(TermValueList<?> dictionary) {
    this.dictionary = dictionary;
    
  
  }
  @Override
  public int getLength() {
      return length;
  }

  @Override
  public int getValueIndex(int docId) {
      int index = SortUtil.binarySearch(sortedRegions, 0, sortedRegions.length, docId);
      if (index < 0) {
          index = (index + 1) * -1;
      }
      if (index < 0 || index >= sortedRegions.length) {
          return -1;
      }
      SortedRegion region = sortedRegions[index];
      for (int k = 0; k < region.maxDocIds.length; k++) {
        if (docId <= region.maxDocIds[k]) {          
          return region.dictionaryIds[k];
        }
      }     
      
      return -1;
  }

  

  @Override
  public TermValueList<?> getDictionary() {
      return dictionary;
  }
  private SortedRegion currentRegion;
  private List<SortedRegion> regions;
  int prevDictionaryId = -1;
  public void add(int docId, int dictionaryValueId) {
     Assert.state(dictionaryValueId >= 0);
    if (currentRegion == null) {
       regions = new ArrayList<SecondarySortedForwardIndex.SortedRegion>();
       currentRegion = createNewRegion();
       regions.add(currentRegion);
     } if (dictionaryValueId < prevDictionaryId) {       
       currentRegion = createNewRegion();
       regions.add(currentRegion);
     }
     currentRegion.add(dictionaryValueId, docId);
     prevDictionaryId = dictionaryValueId;
    
  }
  public SortedRegion createNewRegion() {
    SortedRegion currentRegion = new SortedRegion(dictionary.size());
     return currentRegion;
  }

  
  public ColumnMetadata getColumnMetadata() {
      return columnMetadata;
  }
  
  public void setDictionary(TermValueList<?> dictionary) {
      this.dictionary = dictionary;
  }
  public void setLength(int length) {
      this.length = length;
  }
  public void setColumnMetadata(ColumnMetadata columnMetadata) {
      this.columnMetadata = columnMetadata;
  }
  public void seal(ColumnMetadata columnMetadata) {
     this.length = columnMetadata.getNumberOfElements();
    this.columnMetadata = columnMetadata;
    sortedRegions = regions.toArray(new SortedRegion[regions.size()]);
     regions = null;
     currentRegion = null;
     prevDictionaryId = -1;
     for (SortedRegion region : sortedRegions) {
       region.seal();
    }
  }
  @Override
  public ColumnType getColumnType() {
    return columnMetadata.getColumnType();
  }
  @Override
  public int numberOfSortedRegions() {
    return sortedRegions.length;
  }
  @Override
  public SortedRegion[] getSortedRegions() {
    return sortedRegions;
  }
}