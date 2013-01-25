package com.senseidb.ba.realtime.facet;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.util.LazyBigIntArray;

public abstract class RealtimeFacetCountCollector extends DefaultFacetCountCollector {

  public RealtimeFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec) {
    super(name, dataCache, docBase, sel, ospec);
    _countlength = dataCache.valArray.size();
    if (_countlength <= 3096)
    {
      _count = new LazyBigIntArray(_countlength);
    } else
    {
      _count = intarraymgr.get(_countlength);
      intarraylist.add(_count);
    }
  }

}
