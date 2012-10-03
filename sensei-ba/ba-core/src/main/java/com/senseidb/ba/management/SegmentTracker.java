package com.senseidb.ba.management;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.index1.SegmentPersistentManager;
import com.senseidb.ba.util.TarGzCompressionUtils;

public class SegmentTracker {
  private static Logger logger = Logger.getLogger(SegmentTracker.class);
  private File indexDir;
  private List<String> activeSegments = new ArrayList<String>();
  private Map<String, SegmentToZoieReaderAdapter> segmentsMap = new HashMap<String, SegmentToZoieReaderAdapter>();
  private List<String> loadingSegments = new CopyOnWriteArrayList<String>();
  private  ExecutorService executorService;
  protected Object globalLock = new Object();
  protected static Configuration configuration = new Configuration(false);
  protected Map<String, AtomicInteger> referenceCounts = new HashMap<String, AtomicInteger>();
  private IndexReaderDecorator senseiDecorator;
  private volatile boolean isStopped;
  private FileSystem fileSystem;
  @SuppressWarnings("rawtypes")
  public void start(File indexDir, FileSystem fileSystem, IndexReaderDecorator senseiDecorator, ExecutorService executorService) {
   
    this.indexDir = indexDir;
    this.fileSystem = fileSystem;
    this.senseiDecorator = senseiDecorator;
    this.executorService = executorService;
    synchronized (globalLock) {
      for (File file : indexDir.listFiles()) {
        try {
          if (!file.isDirectory()) {
            continue;
          }
          if (!new File(file, "finishedLoading").exists()) {
            logger.warn("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
            FileUtils.deleteDirectory(file);
            continue;
          }
          GazelleIndexSegmentImpl indexSegment = SegmentPersistentManager.read(file, false);
          if (indexSegment == null) {
            logger.warn("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
            FileUtils.deleteDirectory(file);
            continue;
          }
          segmentsMap.put(file.getName(), new SegmentToZoieReaderAdapter(indexSegment, file.getName(), senseiDecorator));
          activeSegments.add(file.getName());
          referenceCounts.put(file.getName(), new AtomicInteger(1));
        } catch (Exception ex) {
          logger.error("Couldn't load the segment - " + file.getAbsolutePath(), ex);
        }
      }
    }
  }

  public void addSegment(final String segmentId, final SegmentInfo segmentInfo) {
    if (isStopped) {
      logger.warn("Could not add segment, as the tracker is already stopped");
      return;
    }
    loadingSegments.add(segmentId);
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        instantiateSegment(segmentId, segmentInfo);
       
      }
    });
  }

  public void instantiateSegment(String segmentId, SegmentInfo segmentInfo) {
    String uri = segmentInfo.getPathUrl();
    try {

      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        TarGzCompressionUtils.unTar(new File(uri), indexDir);
        File file = new File(indexDir, segmentId);
        Thread.sleep(100);
        if (!file.exists()) {
          throw new IllegalStateException("The index directory hasn't been created");
        }
        new File(file, "finishedLoading").createNewFile();
        GazelleIndexSegmentImpl indexSegment = SegmentPersistentManager.read(file, false);
        logger.info("Loaded the new segment " + segmentId + " with " + indexSegment.getLength() + " elements");
        if (indexSegment == null) {
          FileUtils.deleteDirectory(file);
          throw new IllegalStateException("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
        }
        synchronized (globalLock) {
          
          segmentsMap.put(segmentId, new SegmentToZoieReaderAdapter(indexSegment, segmentId, senseiDecorator));
          activeSegments.add(segmentId);
          loadingSegments.remove(segmentId);
          referenceCounts.put(segmentId, new AtomicInteger(1));
        }
      }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }
  public void incrementCount(final String segmentId) {
    referenceCounts.get(segmentId).incrementAndGet();
  }
  public void decrementCount(final String segmentId) {
    AtomicInteger count = referenceCounts.get(segmentId);
    
    if (count.get() == 1) {
      synchronized (globalLock) {
        if (count.get() == 1) {
          segmentsMap.remove(segmentId);
          activeSegments.remove(segmentId);
          referenceCounts.remove(segmentId);
        }
      }  
      logger.info("Segment " + segmentId + " has been deleted");
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          FileUtils.deleteQuietly(new File(indexDir, segmentId));
          logger.info("The index directory for the segment " + segmentId + " has been deleted");
        }
      });
      
    } else {
      count.decrementAndGet();
    }
  }
  
  public void stop() {
    synchronized (globalLock) {
      executorService.shutdown();
      isStopped = true;
      activeSegments.clear();
      segmentsMap.clear();
      loadingSegments.clear();
    }
    try {
      executorService.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }

  }
  public void removeSegment(String segmentId) {
    if (isStopped) {
      logger.warn("Could not remove segment, as the tracker is already stopped");
      return;
    }
    decrementCount(segmentId);
  }

  public List<String> getActiveSegments() {
    return activeSegments;
  }

  public List<String> getLoadingSegments() {
    return loadingSegments;
  }
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders()  {
    List<ZoieIndexReader<BoboIndexReader>> ret = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
    synchronized(globalLock) {
      for (SegmentToZoieReaderAdapter adapter : segmentsMap.values()) {
        incrementCount(adapter.getSegmentId());
        ret.add(adapter);
      }
    }
    return ret;
  }
  public List<ZoieIndexReader<BoboIndexReader>> getIndexReadersWithNoCounts()  {
    List<ZoieIndexReader<BoboIndexReader>> ret = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
    synchronized(globalLock) {
      for (SegmentToZoieReaderAdapter adapter : segmentsMap.values()) {
        ret.add(adapter);
      }
    }
    return ret;
  }
  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> ret) {
    synchronized(globalLock) {
      for (ZoieIndexReader<BoboIndexReader> adapter : ret) {
        decrementCount(((SegmentToZoieReaderAdapter) adapter).getSegmentId());
      }
    }

  }
}
