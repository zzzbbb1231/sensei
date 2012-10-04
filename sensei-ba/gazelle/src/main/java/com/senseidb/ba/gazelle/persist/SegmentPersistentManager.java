package com.senseidb.ba.gazelle.persist;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.SortedForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class SegmentPersistentManager {

  public static void flush(GazelleIndexSegmentImpl segment, File baseDir) throws IOException, ConfigurationException {
    DictionaryPersistentManager.flush(segment.getColumnMetadataMap(), segment.getDictionaries(), baseDir);
    MetadataPersistentManager.flush(segment.getColumnMetadataMap(), baseDir);
    HashMap<String, Integer> dictionarySizeMap = new HashMap<String, Integer>();
    for (String column : segment.getDictionaries().keySet()) {
      dictionarySizeMap.put(column, segment.getDictionary(column).size());
    }
    List<GazelleForwardIndexImpl> forwardIndexes = new ArrayList<GazelleForwardIndexImpl>();
    for (ForwardIndex forwardIndex : segment.getForwardIndexes().values()) {
      if (forwardIndex instanceof GazelleForwardIndexImpl) {
        forwardIndexes.add((GazelleForwardIndexImpl) forwardIndex);
      } else if (forwardIndex instanceof SortedForwardIndex){
        SortedForwardIndexImpl sortedIndex = (SortedForwardIndexImpl) forwardIndex;
       SortedIndexPersistentManager.persist(new File(baseDir, sortedIndex.getColumnMetadata().getName() + ".ranges"), sortedIndex);
      } else {
        throw new UnsupportedOperationException(forwardIndex.getClass().getCanonicalName());
      }
    }
    ForwardIndexPersistentManager.flush(forwardIndexes, baseDir);
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
      File file = new File(indexDir, GazelleUtils.INDEX_FILENAME);
      for (ColumnMetadata columnMetadata : metadataMap.values()) {
        TermValueList dictionary = dictionaries.get(columnMetadata.getName());
        if (columnMetadata.isSorted()) {
          SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionaries.get(columnMetadata.getName()), new int[columnMetadata.getNumberOfDictionaryValues()], new int[columnMetadata.getNumberOfDictionaryValues()], columnMetadata.getNumberOfElements(), columnMetadata);
          SortedIndexPersistentManager.readMinMaxRanges(new File(indexDir, columnMetadata.getName() + ".ranges"), sortedForwardIndexImpl);
          ret.put(columnMetadata.getName(), sortedForwardIndexImpl);
        } else {
          GazelleForwardIndexImpl gazelleForwardIndexImpl = new GazelleForwardIndexImpl(columnMetadata.getName(), ForwardIndexPersistentManager.readForwardIndex(columnMetadata, file, mode), dictionary, columnMetadata);
          ret.put(columnMetadata.getName(), gazelleForwardIndexImpl);
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
