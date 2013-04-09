package com.senseidb.ba.gazelle.persist;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.custom.GazelleCustomIndex;
import com.senseidb.ba.gazelle.custom.GazelleCustomIndexRegistry;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;

public class SegmentPersistentManager {
  private static Logger logger = Logger.getLogger(SegmentPersistentManager.class);
  private static final String VERSION = "001";

  public static void flushToDisk(GazelleIndexSegmentImpl segment, File baseDir)  {
    try {
      if (!baseDir.exists()) {
        baseDir.mkdirs();
      }
      flush(segment, baseDir.getAbsolutePath(), FileSystemMode.DISK, null);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void flushToHadoop(GazelleIndexSegmentImpl segment, String baseDir, FileSystem fs) {
    try {
      flush(segment, baseDir, FileSystemMode.HDFS, fs);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static void flush(GazelleIndexSegmentImpl segment, String baseDir, FileSystemMode mode, FileSystem fs) throws IOException,
      ConfigurationException, IllegalAccessException {
    segment.getSegmentMetadata().put("segment.index.version", VERSION);
    segment.setAssociatedDirectory(baseDir);
    DictionaryPersistentManager.flush(segment.getColumnMetadataMap(), segment.getDictionaries(), baseDir, mode, fs);
    for (GazelleCustomIndex customIndex : segment.getCustomIndexes().values()) {
      customIndex.getCreator().flushIndex(baseDir, mode, fs);
    }
    MetadataPersistentManager.flush(segment, baseDir, mode, fs);
    HashMap<String, Integer> dictionarySizeMap = new HashMap<String, Integer>();
    for (String column : segment.getDictionaries().keySet()) {
      dictionarySizeMap.put(column, segment.getDictionary(column).size());
    }
    for (String column : segment.getForwardIndexes().keySet()) {
     if (segment.getCustomIndexes().containsKey(column)) {
       continue;
     }
      ForwardIndex forwardIndex = segment.getForwardIndexes().get(column);
      if (forwardIndex instanceof GazelleForwardIndexImpl) {
        ForwardIndexPersistentManager.flush((GazelleForwardIndexImpl) forwardIndex, baseDir, mode, fs);
      } else if (forwardIndex instanceof SortedForwardIndex) {
        SortedForwardIndexImpl sortedIndex = (SortedForwardIndexImpl) forwardIndex;
        String fileName = baseDir + "/" + sortedIndex.getColumnMetadata().getName() + ".ranges";
        SortedIndexPersistentManager.flush(fileName, sortedIndex, mode, fs);
      } else if (forwardIndex instanceof SecondarySortedForwardIndex) {
        SecondarySortedForwardIndex sortedIndex = (SecondarySortedForwardIndex) forwardIndex;
        String fileName = baseDir + "/" + sortedIndex.getColumnMetadata().getName() + ".srtranges";
        SecondarySortedIndexPersistentManager.flush(fileName, sortedIndex, mode, fs);
      } else if (forwardIndex instanceof MultiValueForwardIndexImpl1) {
        MultiValueForwardIndexImpl1 multiValueForwardIndex = (MultiValueForwardIndexImpl1) forwardIndex;

        multiValueForwardIndex.getCompressedMultiArray().flushToFile(baseDir, multiValueForwardIndex.getColumnMetadata().getName(), mode,
            fs);
      } else {
        throw new UnsupportedOperationException(forwardIndex.getClass().getCanonicalName());
      }
    }

  }

  public static GazelleIndexSegmentImpl read(File indexDir, ReadMode mode, String[] invertedColumns)  {
    try {
      File file = new File(indexDir, GazelleUtils.METADATA_FILENAME);
      PropertiesConfiguration config = new PropertiesConfiguration(file);
      SegmentMetadata globalProperties = MetadataPersistentManager.readSegmentMetadata(config);
      HashMap<String, ColumnMetadata> metadataMap = MetadataPersistentManager.readFromFile(config);
      Map<String, Map<String, ColumnMetadata>> customIndexesMetadata = new HashMap<String, Map<String, ColumnMetadata>>();
      Map<String, String> customIndexes = new HashMap<String, String>();
      for (String column : metadataMap.keySet()) {
        ColumnMetadata columnMetadata = metadataMap.get(column);
        if (columnMetadata.getCustomIndexerType() != null) {
          String customIndexerType = columnMetadata.getCustomIndexerType();
          if (!customIndexesMetadata.containsKey(customIndexerType)) {
            customIndexesMetadata.put(customIndexerType, new HashMap<String, ColumnMetadata>());
          }
          customIndexesMetadata.get(customIndexerType).put(column, metadataMap.get(column));
          customIndexes.put(column, customIndexerType);
        }
      }
      metadataMap.keySet().removeAll(customIndexes.keySet());
      Map<String, TermValueList> dictionaries = getTermValueListMap(metadataMap, indexDir);
      Map<String, ForwardIndex> forwardIndexesMap = getForwardIndexesMap(metadataMap, dictionaries, indexDir, mode);
      GazelleIndexSegmentImpl ret = new GazelleIndexSegmentImpl(metadataMap,
          forwardIndexesMap, dictionaries, globalProperties, forwardIndexesMap.values().iterator()
              .next().getLength(), invertedColumns);

      for (String customIndex : customIndexesMetadata.keySet()) {
        GazelleCustomIndex gazelleCustomIndex = GazelleCustomIndexRegistry.get(customIndex);
        Map<String, ColumnMetadata> properties = customIndexesMetadata.get(customIndex);
        gazelleCustomIndex.init(properties);
        gazelleCustomIndex.load(indexDir, mode);
        ret.addCustomIndex(gazelleCustomIndex, properties);
      }
      ret.setAssociatedDirectory(indexDir.getAbsolutePath());
      return ret;
    } catch (Exception ex) {
      logger.error("Couldn't read the segment", ex);
      return null;
    }
  }

  public static GazelleIndexSegmentImpl read(File indexDir, ReadMode mode)  {
    return read(indexDir, mode, new String[0]);
  }

  public static Map<String, String> getSegmentMetadata(PropertiesConfiguration config) {
    Map<String, String> globalProperties = new HashMap<String, String>();
    Iterator keys = config.getKeys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      if (!key.startsWith("column")) {
        Object value = config.getProperty(key);
        if (value != null) {
          globalProperties.put(key, value.toString());
        }
      }
    }
    return globalProperties;
  }

  private static Map<String, ForwardIndex> getForwardIndexesMap(Map<String, ColumnMetadata> metadataMap,
      Map<String, TermValueList> dictionaries, File indexDir, ReadMode mode) {
    Map<String, ForwardIndex> ret = new HashMap<String, ForwardIndex>();
    try {
      for (ColumnMetadata columnMetadata : metadataMap.values()) {
        if (columnMetadata.getCustomIndexerType() != null) {
          continue;
        }
        TermValueList dictionary = dictionaries.get(columnMetadata.getName());
        if (columnMetadata.isSorted()) {
          SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionaries.get(columnMetadata.getName()),
              new int[columnMetadata.getNumberOfDictionaryValues()], new int[columnMetadata.getNumberOfDictionaryValues()],
              columnMetadata.getNumberOfElements(), columnMetadata);
          SortedIndexPersistentManager.readMinMaxRanges(new File(indexDir, columnMetadata.getName() + ".ranges"), sortedForwardIndexImpl);
          ret.put(columnMetadata.getName(), sortedForwardIndexImpl);
        } else if (columnMetadata.isSecondarySorted()) {
          ret.put(
              columnMetadata.getName(),
              new SecondarySortedForwardIndexImpl(dictionaries.get(columnMetadata.getName()), SecondarySortedIndexPersistentManager
                  .readMinMaxRanges(new File(indexDir, columnMetadata.getName() + ".srtranges"),
                      columnMetadata.getNumberOfDictionaryValues()), columnMetadata.getNumberOfElements(), columnMetadata));
        } else if (!columnMetadata.isMulti()) {
          GazelleForwardIndexImpl gazelleForwardIndexImpl = new GazelleForwardIndexImpl(columnMetadata.getName(),
              ForwardIndexPersistentManager.readForwardIndex(columnMetadata, new File(indexDir, columnMetadata.getName() + ".fwd"), mode),
              dictionary, columnMetadata);
          ret.put(columnMetadata.getName(), gazelleForwardIndexImpl);
        } else if (columnMetadata.isMulti()) {
          MultiValueForwardIndexImpl1 forwardIndexImpl = new MultiValueForwardIndexImpl1(columnMetadata.getName(),
              CompressedMultiArray.readFromFile(indexDir, columnMetadata.getName(), columnMetadata.getBitsPerElement(), mode), dictionary,
              columnMetadata);
          ret.put(columnMetadata.getName(), forwardIndexImpl);
        }

      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return ret;
  }

  private static Map<String, TermValueList> getTermValueListMap(Map<String, ColumnMetadata> metadataMap, File indexDir) throws IOException {
    HashMap<String, TermValueList> dictionaryMap = new HashMap<String, TermValueList>();
    for (String column : metadataMap.keySet()) {
      if (metadataMap.get(column).getCustomIndexerType() != null) {
        continue;
      }
      File file = new File(indexDir, (column + ".dict"));
      TermValueList list = DictionaryPersistentManager.read(file, metadataMap.get(column).getColumnType(), metadataMap.get(column)
          .getNumberOfDictionaryValues());
      dictionaryMap.put(column, list);
    }
    return dictionaryMap;
  }
}
