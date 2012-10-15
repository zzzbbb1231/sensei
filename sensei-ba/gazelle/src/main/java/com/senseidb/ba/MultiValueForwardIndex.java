package com.senseidb.ba;

import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public interface MultiValueForwardIndex extends ForwardIndex{
    public MultiFacetIterator getIterator();
    public int randomRead(int[] buffer, int index);
    public int getMaxNumValuesPerDoc();
}
