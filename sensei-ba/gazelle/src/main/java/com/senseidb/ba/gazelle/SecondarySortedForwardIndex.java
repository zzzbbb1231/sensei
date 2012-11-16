package com.senseidb.ba.gazelle;

import com.browseengine.bobo.facets.data.TermValueList;

public interface SecondarySortedForwardIndex extends MetadataAware {
    public TermValueList<?> getDictionary();
    public SortedRegion[] getSortedRegions();
    public int numberOfSortedRegions();
    public static class SortedRegion {
      public int[] minDocIds;
      public int[] maxDocIds;
      public int maxDocId;
      
      public SortedRegion(int[] minDocIds, int[] maxDocIds) {
        super();
        this.minDocIds = minDocIds;
        this.maxDocIds = maxDocIds;
        
      }

      public void init() {
        for (int i = 0; i < maxDocIds.length; i++) {
          if (maxDocId < maxDocIds[i]) {
            maxDocId = maxDocIds[i];
          }
        }
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
