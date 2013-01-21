package com.senseidb.ba.management.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;



import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;

public class SimpleIndexFactory extends AbstractFakeZoie  {

  private final Object globalLock;
  private final List<SegmentToZoieReaderAdapter> readers;

  @SuppressWarnings("rawtypes")
  public SimpleIndexFactory(List<SegmentToZoieReaderAdapter> readers, Object globalLock) {
    this.readers = readers;
    this.globalLock = globalLock;
  }

 

  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders()  {
    synchronized (globalLock) {
      return new ArrayList(readers);
    }
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    // TODO Auto-generated method stub

  }

  public List<SegmentToZoieReaderAdapter> getReaders() {
    return readers;
  }

public Object getGlobalLock() {
    return globalLock;
}
  
}
