package com.senseidb.ba.gazelle;

import com.browseengine.bobo.facets.data.TermValueList;

public interface SortedForwardIndex {
    public TermValueList<?> getDictionary();
    public int[] getMinDocIds();
    public int[] getMaxDocIds();
}
