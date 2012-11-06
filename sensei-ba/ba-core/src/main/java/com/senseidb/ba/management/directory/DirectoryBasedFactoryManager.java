package com.senseidb.ba.management.directory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import proj.zoie.api.Zoie;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;

import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.creators.SegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.plugins.ZeusIndexReaderDecorator;
import com.senseidb.ba.util.TarGzCompressionUtils;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.conf.ZoieFactoryFactory;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.node.SenseiIndexReaderDecorator;
import com.senseidb.search.node.SenseiZoieFactory;
import com.senseidb.util.Pair;

public class DirectoryBasedFactoryManager extends SenseiZoieFactory implements SenseiPlugin, ZoieFactoryFactory {
  private static Logger logger = Logger.getLogger(DirectoryBasedFactoryManager.class);  
     private File directory;
    private File explodeDirectory;
    private boolean duplicateForAllPartitions = false;
    private Map<String, SegmentToZoieReaderAdapter> segmentsMap = new HashMap<String, SegmentToZoieReaderAdapter>();
    private Map<String, String> keyToAbsoluteFilePath = new HashMap<String, String>();
    private Set<String> loadingSegments = new HashSet<String>();
    private SenseiIndexReaderDecorator decorator;
    protected Object globalLock = new Object();
    private ExecutorService executorService = Executors.newFixedThreadPool(5);
    private Timer timer = new Timer();
    private Map<Integer, MapBasedIndexFactory> readers = new HashMap<Integer, MapBasedIndexFactory>();
    private int maxPartition;
    private AtomicInteger counter = new AtomicInteger();
    public DirectoryBasedFactoryManager() {
      super(null, null, null, null, null);
    }
    
    public void start() {
      try {
        cleanUpIndexesWithoutFinishedLoadingTag(explodeDirectory);
        initReadySegments(directory);
        initReadySegments(explodeDirectory);
        scanForNewSegments();
        timer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            scanForNewSegments();
            
          }
        }, 15 * 1000, 15 * 1000);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    public synchronized void scanForNewSegments() {
      try {
        final Map<String, Pair<FileType, File>> availableKeys = getAvailableKeys(directory);
        final Set<String> segmentsToAdd = new HashSet<String>();
        final Set<String> segmentsToRemove = new HashSet<String>();
        synchronized(globalLock) {
          for (String key : availableKeys.keySet()) {
            if (!loadingSegments.contains(key) && ! segmentsMap.containsKey(key)) {
              segmentsToAdd.add(key);              
            }
          }
          loadingSegments.addAll(segmentsToAdd);
          for (String key : segmentsMap.keySet()) {
            if (!availableKeys.containsKey(key)) {
              segmentsToRemove.add(key);
            }
          }
        }
        for (final String key : segmentsToAdd) {
          executorService.submit(new Runnable() {
            @Override
            public void run() {
              Pair<FileType, File> pair = availableKeys.get(key);
              instantiateSegment(key, pair.getSecond(), pair.getFirst());
            }
          });
        }
        for (final String key : segmentsToRemove) {
          executorService.submit(new Runnable() {
            @Override
            public void run() {
              removeSegment(key);
            }
          });
        }
      } catch (Exception e) {        
        logger.error(e.getMessage(), e);
      }
    }
    public void removeSegment(String segmentId) {
      logger.info("Removing segment - " + segmentId);
      String path = null;
      try {
      synchronized(globalLock) {
        segmentsMap.remove(segmentId);
        path = keyToAbsoluteFilePath.remove(segmentId);
        loadingSegments.remove(segmentId);        
          for (MapBasedIndexFactory mapBasedIndexFactory : readers.values()) {
            mapBasedIndexFactory.getReaders().remove(segmentId);
          }
        
      }
      if (path != null) {
        File dir = new File(path);
        if (dir.exists()) {
          FileUtils.deleteDirectory(dir);
        }
      }
      } catch (Exception e) {        
       logger.error("Failed to delete the dir - " + path, e);
      }
    }
    public void instantiateSegment(String segmentId, File file, FileType fileType) {      
      GazelleIndexSegmentImpl gazelleIndexSegmentImpl = null;
      try {
      File targetDir = new File(explodeDirectory, segmentId);
      if (fileType == FileType.AVRO) {
        gazelleIndexSegmentImpl = SegmentCreator.readFromAvroFile(file);
        SegmentPersistentManager.flushToDisk(gazelleIndexSegmentImpl, targetDir);
        new File(targetDir, "finishedLoading").createNewFile();
      } else if (fileType == FileType.COMPRESSED_GAZELLE) {
        TarGzCompressionUtils.unTar(file, explodeDirectory);       
        Thread.sleep(100);
        if (!targetDir.exists()) {
          throw new IllegalStateException("The index directory hasn't been created");
        }
        new File(targetDir, "finishedLoading").createNewFile();
        gazelleIndexSegmentImpl = SegmentPersistentManager.read(targetDir, ReadMode.DBBuffer);
      } else if (fileType == FileType.GAZELLE){
        targetDir = file;
        gazelleIndexSegmentImpl = SegmentPersistentManager.read(file, ReadMode.DBBuffer);
      }
      int hash = Math.abs(counter.incrementAndGet()) % maxPartition;
      MapBasedIndexFactory mapBasedIndexFactory = readers.get(hash);
      SegmentToZoieReaderAdapter adapter = new SegmentToZoieReaderAdapter(gazelleIndexSegmentImpl, segmentId, decorator);
      if (duplicateForAllPartitions) {
        synchronized (globalLock) {        
          segmentsMap.put(segmentId, adapter);
          keyToAbsoluteFilePath.put(segmentId, targetDir.getAbsolutePath());
          loadingSegments.remove(segmentId);
          for (MapBasedIndexFactory factory : readers.values()) {
            factory.getReaders().put(segmentId, adapter);
          }
          }
        }
       else {
        synchronized (globalLock) {        
          segmentsMap.put(segmentId, adapter);
          keyToAbsoluteFilePath.put(segmentId, targetDir.getAbsolutePath());
          loadingSegments.remove(segmentId);
          if (mapBasedIndexFactory != null) {
            mapBasedIndexFactory.getReaders().put(segmentId, adapter);
          }
        }
      }
      logger.info("Created the new segment - " + segmentId + ", in the directory " + targetDir.getAbsoluteFile()+ ", the source is "+ file.getAbsoluteFile());
      logger.info("the new segment - " + segmentId + " contains " + gazelleIndexSegmentImpl.getLength() + " elements");
      } catch (Throwable e) {
        logger.error("Failed to initialize the segment data - " + file.getAbsolutePath(), e);
        synchronized (globalLock) {  
          loadingSegments.remove(segmentId);
        }
      }
    }
    
    
    public void initReadySegments(File dir) throws ConfigurationException, IOException {
      for (String key : getGazelleIndexes(dir)) {
        File file = new File(dir, key);
        logger.info("Loading the index - " + file.getAbsolutePath());
        instantiateSegment(key, file, FileType.GAZELLE);        
        
      }
    }
    @Override
    public SenseiZoieFactory<?> getZoieFactory(File idxDir, ZoieIndexableInterpreter<?> interpreter, SenseiIndexReaderDecorator decorator,
        ZoieConfig config) {
      return this;
    }
    
    @Override
    public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
      String dirStr =  pluginRegistry.getConfiguration().getString("sensei.index.directory");
      String duplicateForAllPartitionsStr =  config.get("duplicateForAllPartitions");
      if ("true".equalsIgnoreCase(duplicateForAllPartitionsStr)) {
        duplicateForAllPartitions = true;
      }
      Assert.notNull(dirStr, "The property 'sensei.index.directory' should be defined");
      directory = new File(dirStr);
      if (!directory.exists()) {
        directory.mkdirs();
      }
      explodeDirectory = new File(directory, "exploded");
      if (!explodeDirectory.exists()) {
        explodeDirectory.mkdirs();
      }
      String partStr = pluginRegistry.getConfiguration().getString(SenseiConfParams.PARTITIONS);
      String[] partitionArray = partStr.split("[,\\s]+");
      try {
        int[] partitions = SenseiServerBuilder.buildPartitions(partitionArray);
        maxPartition = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id", 0) + 1;
        for (int i : partitions) {
          readers.put(i, new MapBasedIndexFactory(new HashMap<String, SegmentToZoieReaderAdapter>(), globalLock));
        }
      } catch (ConfigurationException e) {
       throw new RuntimeException();
      }
      decorator = new ZeusIndexReaderDecorator(pluginRegistry.resolveBeansByListKey(SenseiPluginRegistry.FACET_CONF_PREFIX, FacetHandler.class));
    }
    @Override
    public void stop() {
      timer.cancel();
      executorService.shutdown();
    }
    public List<String> getGazelleIndexes(File directory) {
      List<String> ret = new ArrayList<String>();
      for (File file : directory.listFiles() ) {
        if (!file.isDirectory()) {
          continue;
        }
        if (new File(file, GazelleUtils.METADATA_FILENAME).exists()) {
          ret.add(file.getName());
        }
      }
      return ret;
    }
    public List<String> cleanUpIndexesWithoutFinishedLoadingTag(File directory) throws IOException {
      List<String> ret = new ArrayList<String>();
      for (File file : directory.listFiles() ) {
        if (!file.isDirectory()) {
          continue;
        }
        if (!new File(file, GazelleUtils.METADATA_FILENAME).exists()) {
          continue;
        }
        if (!new File(file, "finishedLoading").exists()) {
          logger.warn("The directory " + file.getAbsolutePath() + " doesn't contain the fully loaded segment");
          FileUtils.deleteDirectory(file);
          ret.add(file.getName());
        }
      }
      return ret;
    }
    public Map<String, Pair<FileType, File>> getAvailableKeys(File directory) throws IOException {
      Map<String, Pair<FileType, File>> ret = new HashMap<String, Pair<FileType, File>>();
      for (File file : directory.listFiles() ) {
        if (file.isDirectory()) {
          if (new File(file, GazelleUtils.METADATA_FILENAME).exists()) {
            ret.put(file.getName(), new Pair(FileType.GAZELLE, file));
          }
          continue;
        }
        if (file.getName().endsWith(".avro")) {
          ret.put(file.getName().substring(0, file.getName().length() - ".avro".length()), new Pair(FileType.AVRO, file));
          continue;
        }
        if (file.getName().endsWith(".tar.gz")) {
          ret.put(file.getName().substring(0, file.getName().length() - ".tar.gz".length()), new Pair(FileType.COMPRESSED_GAZELLE, file));
          continue;
        }
      }
      return ret;
    }
    public static enum FileType{
      AVRO, COMPRESSED_GAZELLE, GAZELLE;
    }
    @Override
    public Zoie getZoieInstance(int nodeId, int partitionId) {
      return readers.get(partitionId);
    }
    @Override
    public File getPath(int nodeId, int partitionId) {
      // TODO Auto-generated method stub
      return null;
    }
    @Override
    public SenseiIndexReaderDecorator getDecorator() {
      return super.getDecorator();
    }

    public File getDirectory() {
      return directory;
    }
    
}
