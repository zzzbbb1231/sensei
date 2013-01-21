package com.senseidb.ba.management.directory;

import java.util.Collection;
import java.util.Comparator;

import javax.management.StandardMBean;

import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.mbean.ZoieAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;

public abstract class AbstractFakeZoie implements Zoie<BoboIndexReader, Object> {

    public AbstractFakeZoie() {
        super();
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
    public Analyzer getAnalyzer() {
        // TODO Auto-generated method stub
        return null;
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
    @Override
    public Comparator<String> getVersionComparator() {
      return new ZoieConfig.DefaultVersionComparator();
    }
}