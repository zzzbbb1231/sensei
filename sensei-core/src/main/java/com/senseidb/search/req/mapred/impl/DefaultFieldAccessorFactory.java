package com.senseidb.search.req.mapred.impl;

import java.util.Set;

import proj.zoie.api.DocIDMapper;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.search.req.SenseiSystemInfo.SenseiFacetInfo;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.FieldAccessorImpl;

public class DefaultFieldAccessorFactory implements FieldAccessorFactory {
  @Override
  public FieldAccessor getAccessor(Set<SenseiFacetInfo> facetInfos, BoboIndexReader boboIndexReader, DocIDMapper mapper) {
    return new FieldAccessorImpl(facetInfos, boboIndexReader, mapper);
  }

}
