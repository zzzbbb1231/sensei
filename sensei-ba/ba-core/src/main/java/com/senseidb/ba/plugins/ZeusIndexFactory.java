package com.senseidb.ba.plugins;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import javax.management.StandardMBean;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.mbean.ZoieAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.JsonDataSource;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.creators.GenericIndexCreator;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
@Deprecated
public class ZeusIndexFactory implements Zoie<BoboIndexReader, Object> {
  List<ZoieIndexReader<BoboIndexReader>> offlineSegments = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
  private final File idxDir;
  private final SenseiIndexReaderDecorator decorator;

  public ZeusIndexFactory(File idxDir, SenseiIndexReaderDecorator decorator) {
    this.idxDir = idxDir;
    this.decorator = decorator;
   
    }

  

  @Override
  public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<Object>> data) throws ZoieException {
    throw new UnsupportedOperationException();

  }

  @Override
  public String getVersion() {
    return null;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return null;
  }

  @Override
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() throws IOException {
    return offlineSegments;
  }

  @Override
  public Analyzer getAnalyzer() {
    return null;
  }

  @Override
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getCurrentReaderVersion() {
    return null;
  }

  @Override
  public void start() {
    try {
      File[] jsonFiles = idxDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.getName().endsWith("json");
      }
    });
    for (File jsonFile : jsonFiles) {
      
      IndexSegment offlineSegment =  GenericIndexCreator.create(new JsonDataSource(jsonFile));
      offlineSegments.add(new SegmentToZoieReaderAdapter(offlineSegment, jsonFile.getName(), decorator));
    }
    for (File directory : idxDir.listFiles()) {
      if (directory.getName().endsWith(".avro")) {
          InputStream inputStream = new FileInputStream(directory) ;
          offlineSegments.add(new SegmentToZoieReaderAdapter(AvroSegmentCreator.readFromAvroFile(directory), directory.getName(), decorator));
          IOUtils.closeQuietly(inputStream);
      }
       if (!directory.isDirectory()) {
        continue;
      }
     
      String[] persistentIndexes = directory.list(new FilenameFilter() {
          
          @Override
          public boolean accept(File dir, String name) {
            return name.contains(GazelleUtils.METADATA_FILENAME);
          }
        });
        if (persistentIndexes.length > 0) {
          offlineSegments.add(new SegmentToZoieReaderAdapter(  SegmentPersistentManager.read(directory, ReadMode.DirectMemory), directory.getName(), decorator));
        }
    }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
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
    // TODO Auto-generated method stub
    return new String[0];
  }

  @Override
  public ZoieAdminMBean getAdminMBean() {
    return null;
  }

  @Override
  public void syncWithVersion(long timeInMillis, String version) throws ZoieException {

  }

  @Override
  public void flushEvents(long timeout) throws ZoieException {

  }

}
