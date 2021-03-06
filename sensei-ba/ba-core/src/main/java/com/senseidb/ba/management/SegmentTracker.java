package com.senseidb.ba.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TarGzCompressionUtils;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class SegmentTracker {
  private static Logger logger = Logger.getLogger(SegmentTracker.class);
  private static final Timer segmentBootstrapTime = Metrics.newTimer(new MetricName(SegmentTracker.class, "segmentBootstrapOnStartUpTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentSuccesfulInstantiateTime = Metrics.newTimer(new MetricName(SegmentTracker.class, "segmentTotalSuccesfulInstantiateTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentFailedInstantiateTime = Metrics.newTimer(new MetricName(SegmentTracker.class, "segmentTotalFailedInstantiateTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentLoadIntoMemoryTime = Metrics.newTimer(new MetricName(SegmentTracker.class, "segmentLoadIntoMemoryTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private static final Timer segmentDownloadTime = Metrics.newTimer(new MetricName(SegmentTracker.class, "segmentDownloadTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  public static final Counter lastPushTime = Metrics.newCounter(SegmentTracker.class, "lastPushTime");

  private static final Counter currentNumberOfSegments = Metrics.newCounter(SegmentTracker.class, "currentNumberOfSegments");
  private static final Counter currentNumberOfDocuments = Metrics.newCounter(SegmentTracker.class, "currentNumberOfDocuments");

  private static final Counter numDeletedSegments = Metrics.newCounter(SegmentTracker.class, "numDeletedSegments");
  private static final Counter segmentDownloadSpeed = Metrics.newCounter(SegmentTracker.class, "segmentDownloadSpeed");
  private static final Counter documentsStoredInInvertedIndex = Metrics.newCounter(SegmentTracker.class, "documentsStoredInInvertedIndex");
  private static final Counter invertedIndexCompressionRate = Metrics.newCounter(SegmentTracker.class, "invertedIndexCompressionRate");
  private static final Counter totalDocumentsStoredInInvertedIndex = Metrics.newCounter(SegmentTracker.class, "totalDocumentsStoredInInvertedIndex");

  private static final Timer bootstrapTimePerPartition = Metrics.newTimer(new MetricName(SegmentTracker.class, "bootstrapTimePerPartition"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);
  private File indexDir;
  private List<String> activeSegments = new ArrayList<String>();
  private Map<String, SegmentToZoieReaderAdapter> segmentsMap = new HashMap<String, SegmentToZoieReaderAdapter>();
  private List<String> loadingSegments = new CopyOnWriteArrayList<String>();
  private ExecutorService executorService;
  protected Object globalLock = new Object();
  protected static Configuration configuration = new Configuration(false);
  protected Map<String, AtomicInteger> referenceCounts = new HashMap<String, AtomicInteger>();
  private IndexReaderDecorator senseiDecorator;
  private volatile boolean isStopped;
  private FileSystem fileSystem;
  private ReadMode readMode;
  private String[] invertedColumns;

  @SuppressWarnings("rawtypes")
  public void start(File indexDir, FileSystem fileSystem, IndexReaderDecorator senseiDecorator, ReadMode readMode, ExecutorService executorService, String[] invertedColumns) {

    this.invertedColumns = invertedColumns;
    start(indexDir, fileSystem, senseiDecorator, readMode, executorService);

  }

  @SuppressWarnings("rawtypes")
  public void start(File indexDir, FileSystem fileSystem, IndexReaderDecorator senseiDecorator, ReadMode readMode, ExecutorService executorService) {

    this.indexDir = indexDir;
    this.fileSystem = fileSystem;
    this.senseiDecorator = senseiDecorator;
    this.readMode = readMode;
    this.executorService = executorService;
    logger.info("Bootstrapping indexes on the startup");
    long time = System.currentTimeMillis();
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
          long elapsedTime = System.currentTimeMillis();
          GazelleIndexSegmentImpl indexSegment = SegmentPersistentManager.read(file, readMode, invertedColumns);
          segmentBootstrapTime.update(System.currentTimeMillis() - elapsedTime, TimeUnit.MILLISECONDS);
          if (indexSegment == null) {
            logger.warn("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
            FileUtils.deleteDirectory(file);
            continue;
          }
          segmentsMap.put(file.getName(), new SegmentToZoieReaderAdapter(indexSegment, file.getName(), senseiDecorator));
          currentNumberOfSegments.inc();
          currentNumberOfDocuments.inc(indexSegment.getLength());
          for (String columnName : indexSegment.getColumnMetadataMap().keySet()) {
            InvertedIndex invertedIndex = indexSegment.getInvertedIndex(columnName);
            if (indexSegment.getInvertedIndex(columnName) == null) {
              continue;
            }
            totalDocumentsStoredInInvertedIndex.inc(invertedIndex.getIndexStatistics().getDocCount());
            documentsStoredInInvertedIndex.inc(invertedIndex.getIndexStatistics().getTrueDocCount());
            invertedIndexCompressionRate.inc(invertedIndex.getIndexStatistics().getCompressedSize());
          }
          activeSegments.add(file.getName());
          referenceCounts.put(file.getName(), new AtomicInteger(1));
          logger.info("Bootstrapped the  segment " + file.getName() + " with " + indexSegment.getLength() + " elements");
        } catch (Exception ex) {
          logger.error("Couldn't load the segment - " + file.getAbsolutePath(), ex);
        }
      }
    }
    bootstrapTimePerPartition.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
    logger.info("Finished index boostrap. Total time = " + (System.currentTimeMillis() - time) / 1000 + "secs");
  }

  public void addSegment(final String segmentId, final SegmentInfo segmentInfo, boolean asynchronous) {
    if (isStopped) {
      logger.warn("Could not add segment, as the tracker is already stopped");
      return;
    }
    loadingSegments.add(segmentId);
    if (asynchronous)
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            GazelleIndexSegmentImpl segment = instantiateSegment(segmentId, segmentInfo);
            if (segment == null) {
              return;
            }
            synchronized (globalLock) {
              if (segment == null) {
                return;
              }
              segmentsMap.put(segmentId, new SegmentToZoieReaderAdapter(segment, segmentId, senseiDecorator));
              markSegmentAsLoaded(segmentId);
              referenceCounts.put(segmentId, new AtomicInteger(1));
            }
            currentNumberOfSegments.inc();
          } catch (Exception ex) {
            logger.error("Couldn't instantiate the segment", ex);
          }
        }
      });
    else {
      GazelleIndexSegmentImpl segment = instantiateSegment(segmentId, segmentInfo);
      synchronized (globalLock) {
        if (segment == null) {
          return;
        }
        try {
          segmentsMap.put(segmentId, new SegmentToZoieReaderAdapter(segment, segmentId, senseiDecorator));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        markSegmentAsLoaded(segmentId);
        referenceCounts.put(segmentId, new AtomicInteger(1));
        currentNumberOfSegments.inc();
      }
    }
  }

  public void markSegmentAsLoaded(final String segmentId) {
    if (!activeSegments.contains(segmentId)) {
      activeSegments.add(segmentId);
    }
    loadingSegments.remove(segmentId);
    if (!referenceCounts.containsKey(segmentId)) {
      referenceCounts.put(segmentId, new AtomicInteger(1));
    }
  }

  public GazelleIndexSegmentImpl instantiateSegment(String segmentId, SegmentInfo segmentInfo) {
    long time = System.currentTimeMillis();
    List<String> uris = new ArrayList<String>(segmentInfo.getPathUrls());
    GazelleIndexSegmentImpl ret = null;
    boolean success = false;
    Collections.shuffle(uris);
    for (String currentUri : uris) {
      currentUri = currentUri.trim();
      logger.info("trying to load segment  + " + segmentId + ", by uri - " + currentUri);
      ret = instantiateSegmentForUri(segmentId, currentUri);
      if (ret != null) {
        success = true;
        logger.info("Succesfully loaded  segment - " + segmentId + " by the uri " + currentUri);
        break;
      } else {
        logger.warn("Couldn't load the segment by the uri " + currentUri);
      }
    }

    long duration = System.currentTimeMillis() - time;
    if (!success) {
      logger.warn("[final]Failed to load the segment - " + segmentId + ", by the collection of uris" + segmentInfo.getPathUrls());
      segmentFailedInstantiateTime.update(duration, TimeUnit.MILLISECONDS);
      logger.error("[final]Failed to load the segment - " + segmentId + ", by the uris -" + segmentInfo.getPathUrls());
    } else {
      segmentSuccesfulInstantiateTime.update(duration, TimeUnit.MILLISECONDS);

    }
    return ret;
  }

  public GazelleIndexSegmentImpl instantiateSegmentForUri(String segmentId, String uri) {
    try {
      List<File> uncompressedFiles = null;
      if (uri.startsWith("hdfs:")) {
        throw new UnsupportedOperationException("Not implemented yet");
      } else {
        if (uri.startsWith("http:")) {

          File tempFile = new File(indexDir, segmentId + ".tar.gz");
          long downloadTime = System.currentTimeMillis();
          FileUploadUtils.getFile(uri, tempFile);
          long duration = System.currentTimeMillis() - downloadTime;
          segmentDownloadTime.update(duration, TimeUnit.MILLISECONDS);
          segmentDownloadSpeed.clear();
          segmentDownloadSpeed.inc((tempFile.length() / (duration * 1000)));
          logger.info("Downloaded file from " + uri);
          uncompressedFiles = TarGzCompressionUtils.unTar(tempFile, indexDir);

          FileUtils.deleteQuietly(tempFile);
        } else {
          uncompressedFiles = TarGzCompressionUtils.unTar(new File(uri), indexDir);
        }
        File file = new File(indexDir, segmentId);
        logger.info("Uncompressed segment into " + file.getAbsolutePath());
        Thread.sleep(100);

        if (uncompressedFiles.size() > 0 && !segmentId.equals(uncompressedFiles.get(0).getName())) {
          if (file.exists()) {
            logger.info("Deleting the directory and recreating it again- " + file.getAbsolutePath());
            FileUtils.deleteDirectory(file);
          }
          File srcDir = uncompressedFiles.get(0);
          logger.warn("The directory - " + file.getAbsolutePath() + " doesn't exist. Would try to rename the dir - " + srcDir.getAbsolutePath() + " to it. The segment id is - " + segmentId);
          FileUtils.moveDirectory(srcDir, file);
          if (!new File(indexDir, segmentId).exists()) {
            throw new IllegalStateException("The index directory hasn't been created");
          } else {
            logger.info("Was able to succesfully rename the dir to match the segmentId - " + segmentId);
          }
        }
        new File(file, "finishedLoading").createNewFile();
        long loadTime = System.currentTimeMillis();

        GazelleIndexSegmentImpl indexSegment = SegmentPersistentManager.read(file, readMode, invertedColumns);

        if (indexSegment != null) {
          logger.info("Loaded the new segment " + segmentId + " with " + indexSegment.getLength() + " elements");
        }

        if (indexSegment == null) {
          FileUtils.deleteDirectory(file);
          throw new IllegalStateException("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
        }
        segmentLoadIntoMemoryTime.update(System.currentTimeMillis() - loadTime, TimeUnit.MILLISECONDS);
        currentNumberOfDocuments.inc(indexSegment.getLength());
        for (String columnName : indexSegment.getColumnMetadataMap().keySet()) {
          InvertedIndex invertedIndex = indexSegment.getInvertedIndex(columnName);
          if (indexSegment.getInvertedIndex(columnName) == null) {
            continue;
          }
          totalDocumentsStoredInInvertedIndex.inc(invertedIndex.getIndexStatistics().getDocCount());
          documentsStoredInInvertedIndex.inc(invertedIndex.getIndexStatistics().getTrueDocCount());
          invertedIndexCompressionRate.inc(invertedIndex.getIndexStatistics().getCompressedSize());
        }
        return indexSegment;
      }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    return null;
  }

  public void incrementCount(final String segmentId) {
    referenceCounts.get(segmentId).incrementAndGet();
  }

  public void decrementCount(final String segmentId) {
    if (!referenceCounts.containsKey(segmentId)) {
      logger.warn("Received command to delte unexisting segment - " + segmentId);
      return;
    }

    AtomicInteger count = referenceCounts.get(segmentId);

    if (count.get() == 1) {
      SegmentToZoieReaderAdapter adapter = null;
      synchronized (globalLock) {
        if (count.get() == 1) {
          adapter = segmentsMap.remove(segmentId);
          activeSegments.remove(segmentId);
          referenceCounts.remove(segmentId);
        }
      }
      if (adapter != null) {
        GazelleIndexSegmentImpl indexSegment = (GazelleIndexSegmentImpl) adapter.getOfflineSegment();
        currentNumberOfSegments.dec();
        currentNumberOfDocuments.dec(indexSegment.getLength());
        numDeletedSegments.inc();
        for (String columnName : indexSegment.getColumnMetadataMap().keySet()) {
          InvertedIndex invertedIndex = indexSegment.getInvertedIndex(columnName);
          if (indexSegment.getInvertedIndex(columnName) == null) {
            continue;
          }
          totalDocumentsStoredInInvertedIndex.dec(invertedIndex.getIndexStatistics().getDocCount());
          documentsStoredInInvertedIndex.dec(invertedIndex.getIndexStatistics().getTrueDocCount());
          invertedIndexCompressionRate.dec(invertedIndex.getIndexStatistics().getCompressedSize());
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

  public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders() {
    List<ZoieIndexReader<BoboIndexReader>> ret = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
    synchronized (globalLock) {
      for (SegmentToZoieReaderAdapter adapter : segmentsMap.values()) {
        incrementCount(adapter.getSegmentId());
        ret.add(adapter);
      }
    }
    return ret;
  }

  public List<ZoieIndexReader<BoboIndexReader>> getIndexReadersWithNoCounts() {
    List<ZoieIndexReader<BoboIndexReader>> ret = new ArrayList<ZoieIndexReader<BoboIndexReader>>();
    synchronized (globalLock) {
      for (SegmentToZoieReaderAdapter adapter : segmentsMap.values()) {
        ret.add(adapter);
      }
    }
    return ret;
  }

  public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> ret) {
    synchronized (globalLock) {
      for (ZoieIndexReader<BoboIndexReader> adapter : ret) {
        decrementCount(((SegmentToZoieReaderAdapter) adapter).getSegmentId());
      }
    }

  }

  public Map<String, SegmentToZoieReaderAdapter> getSegmentsMap() {
    return segmentsMap;
  }

  public Object getGlobalLock() {
    return globalLock;
  }

  public IndexReaderDecorator getSenseiDecorator() {
    return senseiDecorator;
  }
}
