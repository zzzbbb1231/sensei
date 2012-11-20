package com.senseidb.ba.gazelle;

import java.util.Arrays;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.SortUtil;

public interface SecondarySortedForwardIndex extends MetadataAware {
    public TermValueList<?> getDictionary();
    public SortedRegion[] getSortedRegions();
    public int numberOfSortedRegions();
    public static class SortedRegion {
      public int[] minDocIds;
      public int[] maxDocIds;
      public int[] dictionaryIds;
      public int maxDocId = -1;
      public SortedRegion() {       
       
      }
      public SortedRegion(int dictionarySize) {
        super();
        this.minDocIds = new int[dictionarySize];
        this.maxDocIds = new int[dictionarySize];
        Arrays.fill(minDocIds, -1);
        Arrays.fill(maxDocIds, -1);
      }
      public void add(int dictionaryValueId, int docId) {
        if (minDocIds[dictionaryValueId] == -1 || minDocIds[dictionaryValueId] > docId) {
          minDocIds[dictionaryValueId] = docId;
        }
        if (maxDocIds[dictionaryValueId] == -1 || maxDocIds[dictionaryValueId] < docId) {
          maxDocIds[dictionaryValueId] = docId;
        }
      }
      public void seal() {
        int count = 0;
        for (int i = 0; i < maxDocIds.length; i++) {
          if (maxDocIds[i] != -1) {
            count++;
          }
          if (maxDocId < maxDocIds[i]) {
            maxDocId = maxDocIds[i];
          }
        }
        int[] newMinDocIds = new int[count];
        int[] newMaxDocIds = new int[count];
        dictionaryIds = new int[count];
        int index = 0;
        for (int i = 0; i < maxDocIds.length; i++) {
          if (maxDocIds[i] != -1) {
            newMinDocIds[index] = minDocIds[i];
            newMaxDocIds[index] = maxDocIds[i];
            dictionaryIds[index] = i;
            index++;
          }          
        }
        minDocIds = newMinDocIds;
        maxDocIds = newMaxDocIds;
        Assert.state(SortUtil.isSorted(minDocIds));
        Assert.state(SortUtil.isSorted(maxDocIds));
        Assert.state(SortUtil.isSorted(dictionaryIds));
      }
     
      public int[] getMinDocIds() {
        return minDocIds;
      }
      public int[] getMaxDocIds() {
        return maxDocIds;
      }
      
      public void setMinDocIds(int[] minDocIds) {
        this.minDocIds = minDocIds;
      }
      public void setMaxDocIds(int[] maxDocIds) {
        this.maxDocIds = maxDocIds;      
      }
      
    }
}
