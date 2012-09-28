package com.senseidb.ba.management;

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

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.fs.FileSystem;
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

public class BaIndexFactory implements Zoie<BoboIndexReader, Object> {
  List<ZoieIndexReader<BoboIndexReader>> offlineSegments = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
  private final File idxDir;
  private final SenseiIndexReaderDecorator decorator;
  private final ZkClient zkClient;
  private final FileSystem fileSystem;
  private final int partitionId;
  private ZookeeperTracker zookeeperTracker;
  private SegmentTracker segmentTracker;

  public BaIndexFactory(File idxDir, SenseiIndexReaderDecorator decorator, ZkClient zkClient, FileSystem fileSystem, int partitionId) {
    this.idxDir = idxDir;
    this.decorator = decorator;
    this.zkClient = zkClient;
    this.fileSystem = fileSystem;
    this.partitionId = partitionId;
   
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
    segmentTracker = new SegmentTracker();
    segmentTracker.start(idxDir, fileSystem);
    zookeeperTracker = new ZookeeperTracker(zkClient, partitionId, segmentTracker);
    zookeeperTracker.start();
    
  }

  @Override
  public void shutdown() {
    zookeeperTracker.stop();
    segmentTracker.stop();
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