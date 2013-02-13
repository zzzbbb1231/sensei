package com.senseidb.ba.gazelle.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;

public class SecondarySortedForwardIndexImpl implements SingleValueForwardIndex, SecondarySortedForwardIndex, MetadataAware {
  private TermValueList<?> dictionary;
  private int length;
  private ColumnMetadata columnMetadata;
  private SortedRegion[] sortedRegions;

  public SecondarySortedForwardIndexImpl(TermValueList<?> dictionary, SortedRegion[] sortedRegions, int length,
      ColumnMetadata columnMetadata) {
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
  public SingleValueRandomReader getReader() {
    return new SingleValueRandomReader() {
      SingleValueRandomReader randomReader = getReaderInternal();

      @Override
      public int getValueIndex(int docId) {
        int ret = randomReader.getValueIndex(docId);
        if (ret < 0) {
          randomReader = getReaderInternal();
        } else {
          return ret;
        }
        ret = randomReader.getValueIndex(docId);
        if (ret < 0) {
          return ret;
        }
        return ret;
      }
    };
  }

  public SingleValueRandomReader getReaderInternal() {
    return new SingleValueRandomReader() {
      private int currentRegionIndex = -1;
      private final SortedRegion[] regions = sortedRegions;
      private SortedRangeCountFinder countFinder = new SortedRangeCountFinder(regions[0]);
      private SortedRegion currentRegion = null;
      
      private int binarySearchOrGreater(SortedRegion[] in, int start, int end, int target)
      {
        int mid;
        while(start < end)
        {
          mid = (start + end)/2;
          if(in[mid].maxDocId < target)
            start = mid+1;
          else if(in[mid].maxDocId == target)
            return mid;
          else
            end = mid;
        }
        if(in[start].maxDocId >= target)
          return start;
        else
          return -1;
      }     

      public boolean advance(int docid) {
        if (currentRegionIndex == -1) {
          if (regions[0].maxDocId >= docid) {
            currentRegionIndex = 0;
            currentRegion = regions[currentRegionIndex];
            // countFinder.reset(regions[currentRegionIndex]);
            return true;
          } else {
        
              int index = binarySearchOrGreater(regions, currentRegionIndex + 1, regions.length, docid);
              
              if(index != -1){
                  currentRegionIndex = index;
                  currentRegion = regions[currentRegionIndex];
                  countFinder.reset(regions[index]);
                  return true;
              }
              return false;
            
          }
        }
        if (docid <= currentRegion.maxDocId) {
          return true;
        } else {
          int index = binarySearchOrGreater(regions, currentRegionIndex + 1, regions.length, docid);
          
          if(index != -1){
              currentRegionIndex = index;
              currentRegion = regions[currentRegionIndex];
              countFinder.reset(regions[index]);
              return true;
          }
          return false;
        }

      }

      @Override
      public int getValueIndex(int docId) {
        if (!advance(docId)) {
          return -1;
        }
        return countFinder.find(docId);
      }
    };
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
    }
    if (dictionaryValueId < prevDictionaryId) {
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

  public static class SortedRangeCountFinder {

    private int currentIndex = -1;
    private int currentValueId = -1;
    private int[] minDocIds;
    private int[] maxDocIds;
    private int[] dictionaryIds;

    public SortedRangeCountFinder(SortedRegion sortedRegion) {

      minDocIds = sortedRegion.getMinDocIds();
      maxDocIds = sortedRegion.getMaxDocIds();
      dictionaryIds = sortedRegion.dictionaryIds;
    }

    public void reset(SortedRegion sortedRegion) {
      minDocIds = sortedRegion.getMinDocIds();
      maxDocIds = sortedRegion.getMaxDocIds();
      dictionaryIds = sortedRegion.dictionaryIds;
      currentIndex = -1;
      currentValueId = -1;
    }

    public int find(int docid) {
      if (currentIndex == -1) {
        if (maxDocIds[0] >= docid) {
          currentIndex = 0;
          currentValueId = dictionaryIds[0];
        } else {
          currentIndex = Arrays.binarySearch(maxDocIds, docid);
          if (currentIndex < 0) {
            currentIndex = (currentIndex + 1) * -1;
          }
          if (currentIndex >= maxDocIds.length) {
            return -1;
          }
          currentValueId = dictionaryIds[currentIndex];
          return currentValueId;
        }
      } else if (docid > maxDocIds[currentIndex]) {
        while (++currentIndex < maxDocIds.length) {
          if (maxDocIds[currentIndex] >= docid) {
            currentValueId = dictionaryIds[currentIndex];
            return currentValueId;
          }
        }
        return -1;
      }
      return currentValueId;
    }
  }
}