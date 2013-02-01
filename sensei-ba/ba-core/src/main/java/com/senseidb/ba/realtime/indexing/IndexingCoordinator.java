package com.senseidb.ba.realtime.indexing;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.ba.management.directory.SimpleIndexFactory;
import com.senseidb.ba.plugins.ZeusIndexReaderDecorator;
import com.senseidb.ba.realtime.SegmentAppendableIndex;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.indexing.PendingSegmentsIndexFactory.SegmentPersistedListener;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;

public class IndexingCoordinator extends SenseiZoieFactory implements SegmentPersistedListener,SenseiPlugin, ZoieFactoryFactory {
  private static Logger logger = Logger.getLogger(IndexingCoordinator.class);  

  private IndexConfig indexConfig;
  private RealtimeIndexFactory realtimeIndexFactory;
  private PendingSegmentsIndexFactory pendingSegmentsIndexFactory;
  private Map<Integer, SimpleIndexFactory> staticIndexFactories = new HashMap<Integer, SimpleIndexFactory>();
  private File directory;
  private ZeusIndexReaderDecorator decorator;
  private RealtimeIndexingManager indexingManager;
  private Metadata metadata;
  private RealtimeDataProvider realtimeDataProvider;
  public IndexingCoordinator() {
    super(null, null, null, null, null);
  }
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    indexingManager = new RealtimeIndexingManager();
    String dataProviderName = config.get("dataProvider");
    Assert.notNull(dataProviderName, "Property dataProvider should be defined");
     realtimeDataProvider = pluginRegistry.getBeanByName(dataProviderName, RealtimeDataProvider.class);
    Assert.notNull(realtimeDataProvider, "realtimeDataProvider No dataProvider instance with name - " + dataProviderName);
    indexConfig = IndexConfig.valueOf(config,  pluginRegistry);
    metadata = new Metadata(indexConfig.getIndexDir());
    directory = new File(indexConfig.getIndexDir());
    if (!directory.exists()) {
      directory.mkdirs();
    }
    metadata.init();
    //TODO implement versioning
    realtimeDataProvider.init(indexConfig.getSchema(), metadata.version);
    decorator = new ZeusIndexReaderDecorator(pluginRegistry.resolveBeansByListKey(SenseiPluginRegistry.FACET_CONF_PREFIX, FacetHandler.class));
    realtimeIndexFactory = new RealtimeIndexFactory(decorator, indexConfig);
    pendingSegmentsIndexFactory = new PendingSegmentsIndexFactory(this, indexConfig);
    for (int partition = 0; partition < indexConfig.getNumServingPartitions(); partition++) {
      staticIndexFactories.put(partition, new SimpleIndexFactory(new ArrayList<SegmentToZoieReaderAdapter>(), new Object()));
    }
   
    indexingManager.init(indexConfig, realtimeDataProvider, this);
  }
 
  public void start() {
    bootstrap();
    realtimeIndexFactory.start();
    pendingSegmentsIndexFactory.start();
    indexingManager.start();
    realtimeDataProvider.start();
    
  }
  @Override
  public void stop() {
    realtimeDataProvider.stop();
    try {
      indexingManager.stop();
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    try {
      pendingSegmentsIndexFactory.stop();
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
  }
  
    public void segmentSnapshotRefreshed(RealtimeSnapshotIndexSegment indexSegment) {
      logger.debug("RealtimeSegment  refreshed with count " +  indexSegment.getReferencedSegment().getCurrenIndex());
      realtimeIndexFactory.setSnapshot(indexSegment);
    }
     public void segmentFullAndNewCreated(RealtimeSnapshotIndexSegment fullExisingSnapshot, SegmentAppendableIndex oldSegment, SegmentAppendableIndex newSegment) {
       try {      
       oldSegment.setName(indexConfig.getClusterName() + "_" + new SimpleDateFormat("ddMMMyy-HH:mm:ss:SSS").format(new Date(System.currentTimeMillis())));
       logger.info("Segment  " +  oldSegment.getName() + " is full. Containing " + oldSegment.getCurrenIndex() + " elements");
       synchronized(realtimeIndexFactory.getLock()) {
         synchronized(pendingSegmentsIndexFactory.getLock()) {
           realtimeIndexFactory.setSnapshot(newSegment.refreshSearchSnapshot(indexConfig.getIndexObjectsPool()));
           pendingSegmentsIndexFactory.addSegment(oldSegment, fullExisingSnapshot, decorator);
         }
       }
       } catch (Exception ex) {
         logger.error(ex.getMessage(), ex);
         throw new RuntimeException(ex);
       }
    }
    @Override
    public void onSegmentPersisted(SegmentAppendableIndex segmentToProcess, GazelleIndexSegmentImpl persistedSegment) {
      
      logger.info("Segment  " +  segmentToProcess.getName() + " is persisted");
      int partition = Math.abs(segmentToProcess.getName().hashCode()) % indexConfig.getNumServingPartitions();
        SimpleIndexFactory indexFactory = staticIndexFactories.get(partition);
        try {
        synchronized(indexFactory.getGlobalLock()) {
            indexFactory.getReaders().add(new SegmentToZoieReaderAdapter(persistedSegment, segmentToProcess.getName(), decorator));
        }
        indexingManager.getDataProvider().commit(segmentToProcess.getVersion());
        indexingManager.setWaitTillSegmentPersisted(false);
        } catch (IOException e) {
          throw new RuntimeException("Should never happen", e);
        }
    }
  public void bootstrap() {
    try {
    long time = System.currentTimeMillis();
    logger.info("Bootstrapping indexes from the directory - " + directory);
    DirectoryBasedFactoryManager.cleanUpIndexesWithoutFinishedLoadingTag(directory);
    List<String> gazelleIndexes = DirectoryBasedFactoryManager.getGazelleIndexes(directory);
    logger.info("There are  " + gazelleIndexes.size() + " indexes to bootstrap");
    long numDocs = 0;
    for (String gazelleSegment : gazelleIndexes) {
      GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(new File(directory, gazelleSegment), indexConfig.getReadMode());
      numDocs += segment.getLength();
      int partition = Math.abs(gazelleSegment.hashCode()) % indexConfig.getNumServingPartitions();
      SimpleIndexFactory indexFactory = staticIndexFactories.get(partition);
      synchronized(indexFactory.getGlobalLock()) {
        indexFactory.getReaders().add(new SegmentToZoieReaderAdapter(segment, gazelleSegment, decorator));
      }
    }
    logger.info("Boostrapping finished. It took " + (System.currentTimeMillis() - time) + " ms to load  " + numDocs + " docs");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }



  @Override
  public SenseiZoieFactory<?> getZoieFactory(File idxDir, ZoieIndexableInterpreter<?> interpreter, SenseiIndexReaderDecorator decorator,
      ZoieConfig config) {
    return this;
  }

  @Override
  public Zoie getZoieInstance(int nodeId, int partitionId) {
    if (partitionId < indexConfig.getNumServingPartitions()) {
      return staticIndexFactories.get(partitionId);
    }
    if (partitionId == indexConfig.getNumServingPartitions()) {
      return pendingSegmentsIndexFactory;
    }
    if (partitionId == indexConfig.getNumServingPartitions() + 1) {
      return realtimeIndexFactory;
    }
    return null;
  }



  @Override
  public File getPath(int nodeId, int partitionId) {   
    return null;
  }
  public IndexConfig getIndexConfig() {
    return indexConfig;
  }
  @Override
  public Metadata getMetadata() {
    // TODO Auto-generated method stub
    return metadata;
  }

 
    
}
