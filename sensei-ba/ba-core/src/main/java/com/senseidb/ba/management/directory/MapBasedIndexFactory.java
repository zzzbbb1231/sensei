package com.senseidb.ba.management.directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.StandardMBean;

import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.mbean.ZoieAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;

public class MapBasedIndexFactory implements Zoie<BoboIndexReader, Object> {

  private final Object globalLock;
  private final Map<String, SegmentToZoieReaderAdapter> readers;

  @SuppressWarnings("rawtypes")
  public MapBasedIndexFactory(Map<String, SegmentToZoieReaderAdapter> readers, Object globalLock) {
    this.readers = readers;
    this.globalLock = globalLock;
  }

  @Override
  public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<Object>> data) throws ZoieException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return new ZoieConfig.DefaultVersionComparator();
  }

  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders()  {
    synchronized (globalLock) {
      return new ArrayList(readers.values());
    }
  }

  @Override
  public Analyzer getAnalyzer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getCurrentReaderVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void start() {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public StandardMBean getStandardMBean(String name) {
    return null;
  }

  @Override
  public String[] getStandardMBeanNames() {
    return new String[0];
  }

  @Override
  public ZoieAdminMBean getAdminMBean() {
    return null;
  }

  @Override
  public void syncWithVersion(long timeInMillis, String version) throws ZoieException {
    // TODO Auto-generated method stub

  }

  @Override
  public void flushEvents(long timeout) throws ZoieException {
    // TODO Auto-generated method stub

  }

  public Map<String, SegmentToZoieReaderAdapter> getReaders() {
    return readers;
  }
  
}
