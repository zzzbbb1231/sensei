package com.senseidb.ba.realtime.indexing;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.util.Assert;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.StreamDataProvider;
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
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.indexing.PendingSegmentsIndexFactory.SegmentPersistedListener;
import com.senseidb.ba.realtime.indexing.ShardingStrategy.AcceptAllShardingStrategy;
import com.senseidb.ba.realtime.indexing.providers.SenseiProviderAdapter;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.gateway.SenseiGateway;
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

  private ShardingStrategy shardingStrategy;

  private ShardBalancingStrategy shardBalancingStrategy;
  public IndexingCoordinator() {
    super(null, null, null, null, null);
  }
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    indexingManager = new RealtimeIndexingManager();
    String dataProviderName = config.get("dataProvider");
    Assert.notNull(dataProviderName, "Property dataProvider should be defined");
    
    indexConfig = IndexConfig.valueOf(config,  pluginRegistry);
    if (indexConfig.getShardedColumn() == null) {
      shardingStrategy = new ShardingStrategy.AcceptAllShardingStrategy();
    } else {
      shardingStrategy = new FieldShardingStrategy();
      ((FieldShardingStrategy)shardingStrategy).init(indexConfig.getSchema(), indexConfig.getMaxPartitionId(), indexConfig.getShardedColumn());
    }
    realtimeDataProvider = pluginRegistry.getBeanByName(dataProviderName, RealtimeDataProvider.class);
    metadata = new Metadata(indexConfig.getIndexDir());
    if (realtimeDataProvider == null) {
      realtimeDataProvider = initDataProviderFromSenseiGateway(pluginRegistry);
    }
     Assert.notNull(realtimeDataProvider, "realtimeDataProvider No dataProvider instance with name - " + dataProviderName);
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
    shardBalancingStrategy = new ShardBalancingStrategy(indexConfig.getNumServingPartitions());
    indexingManager.init(indexConfig, realtimeDataProvider, this, shardingStrategy);
  }
  
 
  public void start() {
    bootstrap();
    pendingSegmentsIndexFactory.start();
    realtimeDataProvider.start();
    indexingManager.start();
    
    
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
             for (String column : fullExisingSnapshot.getColumnTypes().keySet())  {
                 DictionarySnapshot dictSnapshot = fullExisingSnapshot.getForwardIndex(column).getDictionarySnapshot();
                 dictSnapshot.getResurrectingMarker().incRef();
                 //dictSnapshot.getResurrectingMarker().incRef();
             }
           pendingSegmentsIndexFactory.addSegment(oldSegment, fullExisingSnapshot, decorator);
           realtimeIndexFactory.setSnapshot(newSegment.refreshSearchSnapshot(indexConfig.getIndexObjectsPool()));
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
      int partition = shardBalancingStrategy.chooseShard(persistedSegment);
        SimpleIndexFactory indexFactory = staticIndexFactories.get(partition);
        try {
        synchronized(indexFactory.getGlobalLock()) {
            indexFactory.getReaders().add(new SegmentToZoieReaderAdapter(persistedSegment, segmentToProcess.getName(), decorator));
        }
        indexingManager.getDataProvider().commit(segmentToProcess.getVersion());
        indexingManager.notifySegmentPersisted();        
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
       int partition = shardBalancingStrategy.chooseShard(segment);
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
    throw new UnsupportedOperationException();
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
    return metadata;
  }

  public SenseiProviderAdapter initDataProviderFromSenseiGateway(SenseiPluginRegistry pluginRegistry) {
    SenseiGateway senseiGateway = pluginRegistry.getBeanByFullPrefix(SenseiConfParams.SENSEI_GATEWAY, SenseiGateway.class);
    if (senseiGateway != null) {
      HashSet<Integer> partitions = new HashSet<Integer>();
      partitions.add(0);
      com.senseidb.indexing.ShardingStrategy senseiDummyShardingStrategy = new com.senseidb.indexing.ShardingStrategy() {
        @Override
        public int caculateShard(int maxShardId, JSONObject dataObj) throws JSONException {
          return 0;
        }
        
      };
      try {
        StreamDataProvider dataProvider = senseiGateway.buildDataProvider(indexConfig.getSenseiSchema(), metadata.version, pluginRegistry, senseiDummyShardingStrategy, partitions);
        return new SenseiProviderAdapter(dataProvider);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }
    
}
