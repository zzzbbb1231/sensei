package com.senseidb.ba.mapred;

import java.util.Set;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.Zoie;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.impl.FieldAccessorFactory;

public class BaFieldAccessorFactory implements FieldAccessorFactory {

  @Override
  public FieldAccessor getAccessor(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper) {
    SegmentToZoieReaderAdapter adapter =  (SegmentToZoieReaderAdapter) boboIndexReader.getInnerReader();
    return new BaFieldAccessor(adapter, adapter.getSegmentId());
  }

  @Override
  public IntArray getDocArray(BoboIndexReader boboIndexReader) {
    final SegmentToZoieReaderAdapter adapter =  (SegmentToZoieReaderAdapter) boboIndexReader.getInnerReader();
    return new IntArray() {
      @Override
      public int size() {
        return adapter.getOfflineSegment().getLength();
      }
      @Override
      public int get(int index) {
        return index;
      }
    };
  }
}
