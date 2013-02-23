package com.senseidb.ba.realtime.indexing;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.directory.SimpleIndexWithDeletionFactory;
import com.senseidb.ba.util.TimerService;

public class RetentionController extends TimerTask implements com.senseidb.ba.management.directory.SimpleIndexWithDeletionFactory.GazelleSegmentDeletionListener {
  private static Logger logger = Logger.getLogger(RetentionController.class);  
  private long ttl;
  private static long frequency = 5 * 60 * 1000;
  private final Collection<SimpleIndexWithDeletionFactory> factories;

  public RetentionController(TimeUnit timeUnit, long duration, Collection<SimpleIndexWithDeletionFactory> factories) {
    this.factories = factories;
    ttl = timeUnit.toMillis(duration);
    if (frequency > ttl) {
      frequency = ttl;
    }
    for (SimpleIndexWithDeletionFactory factory : factories) {
      factory.setDeletionListener(this);
    }
    TimerService.timer.schedule(this, frequency, frequency);
  }
  @Override
  public void onDelete(ZoieIndexReader<BoboIndexReader> reader) {
    SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) reader;
    GazelleIndexSegmentImpl gazelleIndexSegmentImpl = (GazelleIndexSegmentImpl) adapter.getOfflineSegment();
    logger.info("Scheduling the indexDirectory - " + gazelleIndexSegmentImpl.getAssociatedDirectory());
    FileUtils.deleteQuietly(new File(gazelleIndexSegmentImpl.getAssociatedDirectory()));
  }
  @Override
  public void run()  {

    long time = System.currentTimeMillis();
    for (SimpleIndexWithDeletionFactory factory : factories) {
      List<ZoieIndexReader<BoboIndexReader>> indexReaders = factory.getIndexReaders();
      try {
      for (ZoieIndexReader<BoboIndexReader> indexReader : indexReaders) {
        SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) indexReader;
        GazelleIndexSegmentImpl gazelleIndexSegmentImpl = (GazelleIndexSegmentImpl) adapter.getOfflineSegment();
        if (time - Long.parseLong(gazelleIndexSegmentImpl.getSegmentMetadata().getEndTime()) > ttl) {
          logger.info("Scheduling to delete the segment - " + adapter.getSegmentId());
          factory.removeSegment(adapter);
        }
      }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
      throw new RuntimeException(ex);
    } finally {
      factory.returnIndexReaders(indexReaders);
    }
    
  }

}
}