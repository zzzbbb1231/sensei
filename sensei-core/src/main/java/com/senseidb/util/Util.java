package com.senseidb.util;

import org.apache.lucene.index.IndexReader;

import com.browseengine.bobo.api.BoboIndexReader;

public class Util {

  public static void ensureBoboReader(IndexReader reader){
    if (!(reader instanceof BoboIndexReader)){
      throw new IllegalArgumentException("IndexReader must be an instance of "+BoboIndexReader.class);
    }
  }
  
}
