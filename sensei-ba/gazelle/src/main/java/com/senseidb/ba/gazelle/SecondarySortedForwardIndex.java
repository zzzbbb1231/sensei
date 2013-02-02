package com.senseidb.ba.gazelle;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.SortUtil;

public interface SecondarySortedForwardIndex extends MetadataAware {
    public TermValueList<?> getDictionary();
    public SortedRegion[] getSortedRegions();
    public int numberOfSortedRegions();
    public static class SortedRegion {
      private IntArrayList tempMinDocIds;
      private IntArrayList tempMaxDocIds;
      private IntArrayList tempdicts;
      private int lastDictId = -1;
      private int currentDictIndex = -1;
      public int[] dictionaryIds;
      public int maxDocId = -1;
      public int[] minDocIds;
      public int[] maxDocIds;
      public SortedRegion() {       
       
      }
      public SortedRegion(int dictionarySize) {
        super();       
      }
      public void add(int dictionaryValueId, int docId) {
        if (tempMinDocIds == null) {
           tempMinDocIds = new IntArrayList();
           tempMaxDocIds = new IntArrayList();
           tempdicts = new IntArrayList();
        }
        
        if (lastDictId == dictionaryValueId) {
         
          if (tempMinDocIds.get(currentDictIndex) > docId) {
            tempMinDocIds.set(currentDictIndex, docId);
          }
          if (tempMaxDocIds.get(currentDictIndex) < docId) {
            tempMaxDocIds.set(currentDictIndex, docId);
          }
        } else {
          currentDictIndex++;
          lastDictId = dictionaryValueId;
          tempMinDocIds.add(currentDictIndex, docId);
          tempMaxDocIds.add(currentDictIndex, docId);
          tempdicts.add(dictionaryValueId);
        }
        
      }
      public void seal() {
        tempdicts.trim();
        tempMinDocIds.trim();
        tempMaxDocIds.trim();
        dictionaryIds = tempdicts.toIntArray();
        minDocIds = tempMinDocIds.toIntArray();
        maxDocIds = tempMaxDocIds.toIntArray();
        maxDocId = maxDocIds[maxDocIds.length - 1];
        tempdicts.clear();
        tempMinDocIds.clear();
        tempMaxDocIds.clear();
        tempdicts = null;
        tempMinDocIds = null;
        tempMaxDocIds = null;
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
