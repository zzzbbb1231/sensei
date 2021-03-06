package com.senseidb.ba.gazelle.impl;

import java.util.Arrays;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.utils.SortUtil;

public class SortedForwardIndexImpl implements SingleValueForwardIndex, SortedForwardIndex, MetadataAware {
    private int[] minDocIds;
    private int[] maxDocIds;
    private TermValueList<?> dictionary;
    private int length;
    private ColumnMetadata columnMetadata;
    public SortedForwardIndexImpl(TermValueList<?> dictionary, int[] minDocIds, int[] maxDocIds, int length, ColumnMetadata columnMetadata) {
        super();
        this.dictionary = dictionary;
        this.minDocIds = minDocIds;
        this.maxDocIds = maxDocIds;
        this.length = length;
        this.columnMetadata = columnMetadata;
       
        minDocIds[0] = -1;
        maxDocIds[0] = -1;
        for (int i = 1; i < minDocIds.length; i++) {
            minDocIds[i] = Integer.MAX_VALUE;
        }
        for (int i = 1; i < maxDocIds.length; i++) {
            maxDocIds[i] = -1;
        }
    }
public SortedForwardIndexImpl() {
    // TODO Auto-generated constructor stub
}
    @Override
    public int[] getMinDocIds() {
        return minDocIds;
    }

    @Override
    public int[] getMaxDocIds() {
        return maxDocIds;
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
      private int currentValueId = -1;
      private int[] minDocIds = SortedForwardIndexImpl.this.minDocIds;
      private int[] maxDocIds = SortedForwardIndexImpl.this.maxDocIds;
      @Override
      public int getValueIndex(int docId) {
        if (currentValueId == -1) {
          if (maxDocIds[1] >= docId) {
            currentValueId = 1;
          } else {
            int index = Arrays.binarySearch(maxDocIds, 1, maxDocIds.length, docId);
            if (index < 0) {
              index = (index + 1) * -1;
            }
            currentValueId = index;
            if (index >= maxDocIds.length) {
              return - 1;
            }
          }
        } else if (docId > maxDocIds[currentValueId]) {
          currentValueId++;
          if (currentValueId == maxDocIds.length) {
            return - 1;
          }
          if (docId > maxDocIds[currentValueId]) {
            int index = Arrays.binarySearch(maxDocIds, currentValueId, maxDocIds.length, docId);
            if (index < 0) {
              index = (index + 1) * -1;
            }
            currentValueId = index;
            if (index >= maxDocIds.length) {
              return - 1;
            }
          }

        }
        return currentValueId;
      }
    };
    }
   

    

    @Override
    public TermValueList<?> getDictionary() {
        return dictionary;
    }
    public void add(int docId, int dictionaryValueId) {
        if (minDocIds[dictionaryValueId] > docId) {
            minDocIds[dictionaryValueId] = docId;
        }
        if (maxDocIds[dictionaryValueId] < docId) {
            maxDocIds[dictionaryValueId] = docId;
        }
    }

    
    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }
    public void setMinDocIds(int[] minDocIds) {
        this.minDocIds = minDocIds;
    }
    public void setMaxDocIds(int[] maxDocIds) {
        this.maxDocIds = maxDocIds;
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
    public void seal() {
        Assert.state(minDocIds.length == maxDocIds.length);
        for (int i = 0; i < minDocIds.length; i++) {
            if (minDocIds[i] > maxDocIds[i]) {
                minDocIds[i] = -1;
                maxDocIds[i] = -1;
            }
        }
        Assert.state(SortUtil.isSorted(minDocIds));
        Assert.state(SortUtil.isSorted(maxDocIds));
    }
    @Override
    public ColumnType getColumnType() {
      return columnMetadata.getColumnType();
    }
    
}
