package com.senseidb.ba.gazelle.persist;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.SortedForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;

public class SegmentPersistentManager {
    public static void flushToDisk(GazelleIndexSegmentImpl segment, File baseDir) throws IOException, ConfigurationException {
        if (!baseDir.exists()) {
            baseDir.mkdirs();
        }
        flush(segment, baseDir.getAbsolutePath(), FileSystemMode.DISK, null);
      }
  public static void flushToHadoop(GazelleIndexSegmentImpl segment, String baseDir, FileSystem fs) throws IOException, ConfigurationException {
    flush(segment, baseDir, FileSystemMode.HDFS, fs);
  }
  
  private static void flush(GazelleIndexSegmentImpl segment, String baseDir, FileSystemMode mode, FileSystem fs) throws IOException, ConfigurationException {
    DictionaryPersistentManager.flush(segment.getColumnMetadataMap(), segment.getDictionaries(), baseDir, mode, fs);
    MetadataPersistentManager.flush(segment.getColumnMetadataMap(), baseDir, mode, fs);
    HashMap<String, Integer> dictionarySizeMap = new HashMap<String, Integer>();
    for (String column : segment.getDictionaries().keySet()) {
      dictionarySizeMap.put(column, segment.getDictionary(column).size());
    }
    for (ForwardIndex forwardIndex : segment.getForwardIndexes().values()) {
      if (forwardIndex instanceof GazelleForwardIndexImpl) {
          ForwardIndexPersistentManager.flush((GazelleForwardIndexImpl) forwardIndex, baseDir, mode, fs);
      } else if (forwardIndex instanceof SortedForwardIndex){
        SortedForwardIndexImpl sortedIndex = (SortedForwardIndexImpl) forwardIndex;
        String fileName = baseDir +"/" +sortedIndex.getColumnMetadata().getName() + ".ranges";
       SortedIndexPersistentManager.flush(fileName, sortedIndex, mode, fs);
      }  else if (forwardIndex instanceof MultiValueForwardIndexImpl){
          MultiValueForwardIndexImpl multiValueForwardIndex = (MultiValueForwardIndexImpl) forwardIndex;
         
          multiValueForwardIndex.getCompressedMultiArray().flushToFile(baseDir, multiValueForwardIndex.getColumnMetadata().getName(), mode, fs);
        } else {
        throw new UnsupportedOperationException(forwardIndex.getClass().getCanonicalName());
      }
    }
    
  }
  
  public static GazelleIndexSegmentImpl read(File indexDir, ReadMode mode) throws ConfigurationException, IOException {
    File file = new File(indexDir, GazelleUtils.METADATA_FILENAME);
    HashMap<String, ColumnMetadata> metadataMap = MetadataPersistentManager.readFromFile(new PropertiesConfiguration(file));
    Map<String, TermValueList> dictionaries = getTermValueListMap(metadataMap, indexDir);
    return new GazelleIndexSegmentImpl(metadataMap, getForwardIndexesMap(metadataMap, dictionaries, indexDir, mode),
        dictionaries, metadataMap.values().iterator().next().getNumberOfElements());
  }

  private static Map<String, ForwardIndex> getForwardIndexesMap(Map<String, ColumnMetadata> metadataMap,  Map<String, TermValueList> dictionaries, File indexDir, ReadMode mode) {
    Map<String, ForwardIndex> ret = new HashMap<String, ForwardIndex>();
    try {
      for (ColumnMetadata columnMetadata : metadataMap.values()) {
        TermValueList dictionary = dictionaries.get(columnMetadata.getName());
        if (columnMetadata.isSorted()) {
          SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionaries.get(columnMetadata.getName()), new int[columnMetadata.getNumberOfDictionaryValues()], new int[columnMetadata.getNumberOfDictionaryValues()], columnMetadata.getNumberOfElements(), columnMetadata);
          SortedIndexPersistentManager.readMinMaxRanges(new File(indexDir, columnMetadata.getName() + ".ranges"), sortedForwardIndexImpl);
          ret.put(columnMetadata.getName(), sortedForwardIndexImpl);
        } else if (!columnMetadata.isMulti()){
          GazelleForwardIndexImpl gazelleForwardIndexImpl = new GazelleForwardIndexImpl(columnMetadata.getName(), ForwardIndexPersistentManager.readForwardIndex(columnMetadata, new File(indexDir, columnMetadata.getName() + ".fwd"), mode), dictionary, columnMetadata);
          ret.put(columnMetadata.getName(), gazelleForwardIndexImpl);
        } else if (columnMetadata.isMulti()) {
            MultiValueForwardIndexImpl forwardIndexImpl = new MultiValueForwardIndexImpl(columnMetadata.getName(), CompressedMultiArray.readFromFile(indexDir, columnMetadata.getName(), columnMetadata.getBitsPerElement(), mode), dictionary, columnMetadata);
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
      File file = new File(indexDir, (column + ".dict"));
      TermValueList list =
          DictionaryPersistentManager.read(file, metadataMap.get(column).getColumnType(), metadataMap.get(column)
              .getNumberOfDictionaryValues());
      dictionaryMap.put(column, list);
    }
    return dictionaryMap;
  }
}
