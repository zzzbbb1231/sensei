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
import java.util.HashSet;
import java.util.List;

import javax.management.StandardMBean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.mbean.ZoieAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.IndexSegment;
import com.senseidb.ba.IndexSegmentCreator;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.index1.InMemoryAvroMapper;
import com.senseidb.ba.index1.SegmentPersistentManager;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

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
      LineIterator lineIterator;
      lineIterator = FileUtils.lineIterator(jsonFile);
      ArrayList<String> docs = new ArrayList<String>();
      while (lineIterator.hasNext()) {
        String car = lineIterator.next();
        if (car != null && car.contains("{"))
          docs.add(car);
      }
      IndexSegment offlineSegment = IndexSegmentCreator.convert(docs.toArray(new String[docs.size()]), new HashSet<String>());
      offlineSegments.add(new SegmentToZoieReaderAdapter(offlineSegment, "", decorator));
    }
    for (File directory : idxDir.listFiles()) {
      if (directory.getName().endsWith(".avro")) {
          InputStream inputStream = new FileInputStream(directory) ;
          offlineSegments.add(new SegmentToZoieReaderAdapter(new InMemoryAvroMapper(directory).build(), directory.getName(), decorator));
          IOUtils.closeQuietly(inputStream);
      }
       if (!directory.isDirectory()) {
        continue;
      }
     
      String[] persistentIndexes = directory.list(new FilenameFilter() {
          
          @Override
          public boolean accept(File dir, String name) {
            return name.contains(SegmentPersistentManager.INDEX_FILE_NAME);
          }
        });
        if (persistentIndexes.length > 0) {
          offlineSegments.add(new SegmentToZoieReaderAdapter( new SegmentPersistentManager().read(directory, false), directory.getName(), decorator));
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
