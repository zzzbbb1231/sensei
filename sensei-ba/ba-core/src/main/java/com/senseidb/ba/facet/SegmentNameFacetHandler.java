package com.senseidb.ba.facet;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.IndexSegment;

public class SegmentNameFacetHandler extends FacetHandler<Serializable> {

  public SegmentNameFacetHandler(String name) {
    super(name);
    
  }

  @Override
  public Serializable load(BoboIndexReader reader) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(BrowseSelection sel, FacetSpec fspec) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    SegmentToZoieReaderAdapter offlineSegment =(SegmentToZoieReaderAdapter) reader.getInnerReader();
    
    return new String[] {offlineSegment.getSegmentId()};
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    // TODO Auto-generated method stub
    return null;
  }

}
