package com.senseidb.ba.gazelle.custom;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.DictionaryPersistentManager;
import com.senseidb.ba.gazelle.utils.IntArray;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class CompositeMetricCustomIndex implements GazelleCustomIndex {
  private List<String> columns;
  private ColumnType columnType;
  GazelleCustomIndexCreator creator;
  private Map<String, Integer> columnIndexes = new HashMap<String, Integer>();
  private Map<String, ColumnMetadata> properties;
  private IntArray compressedIntArray;
  private TermValueList dictionary;
  private static final Timer segmentLoadTime = Metrics.newTimer(new MetricName(CompositeMetricCustomIndex.class ,"segmentLoadTime"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  
  @Override
  public void init(Map<String, ColumnMetadata> properties) {
    this.properties = properties;
    columns = new ArrayList<String>(properties.keySet());
    Collections.sort(columns);
    if (columns.size() > 0) {
      columnType = properties.get(columns.get(0)).getColumnType();
    }
  }

  @Override
  public GazelleCustomIndexCreator getCreator() {
    if (creator == null) {
      creator = new CompositeMetricIndexCreator(columns, columnType);
    }
    return creator;
  }

  @Override
  public void load(File baseDir, ReadMode readMode) {
    int i = 0;
    for (String column : columns) {
      columnIndexes .put(column, i);
      i++;
    }
    long time = System.currentTimeMillis();
    ColumnMetadata columnMetadata = properties.get(columns.get(0));
    compressedIntArray = CompositeMetricsIndexStreamer.getMemoryMapped(baseDir.getAbsolutePath(), columnMetadata.getNumberOfElements(), columns.size(), columnMetadata.getNumberOfDictionaryValues());
     try {
      dictionary = DictionaryPersistentManager.read(new File(baseDir, "modifiedCompositeMetricIndexes.dict"), columnType, columnMetadata.getNumberOfDictionaryValues());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
     segmentLoadTime.update(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
  }

  @Override
  public FacetHandler<GazelleCustomIndex> getFacetHandler(final String column) {
    return new FacetHandler<GazelleCustomIndex>(column) {
      @Override
      public GazelleCustomIndex load(BoboIndexReader reader) throws IOException {
        IndexSegment offlineSegment =(IndexSegment) reader.getFacetData(IndexSegment.class.getSimpleName());
        if (offlineSegment instanceof GazelleIndexSegmentImpl) {
          GazelleCustomIndex gazelleCustomIndex = ((GazelleIndexSegmentImpl)offlineSegment).getCustomIndexes().get(column);
          return gazelleCustomIndex;
        }
        return null;
      }
      @Override
      public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty) throws IOException {
        return null;
      }
      @Override
      public FacetCountCollectorSource getFacetCountCollectorSource(BrowseSelection sel, FacetSpec fspec) {
        return null;
      }
      @Override
      public String[] getFieldValues(BoboIndexReader reader, int id) {
        try {
          GazelleCustomIndex gazelleCustomIndex = load(reader);
          if (gazelleCustomIndex == null) {
            return new String[0];
          }
          return new String[]{gazelleCustomIndex.getDictionary(column).get(gazelleCustomIndex.getReader(column).getValueIndex(id))};
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      @Override
      public DocComparatorSource getDocComparatorSource() {
        return null;
      }
    };
  }
  
  @Override
  public SingleValueRandomReader getReader(String column) {
    final int index = columnIndexes.get(column);
    final int count = columnIndexes.size();
    final int docCount = compressedIntArray.size() / columnIndexes.size();
    return new SingleValueRandomReader() {
      @Override
      public int getValueIndex(int docId) {
        return compressedIntArray.getInt(index * docCount + docId);
      }
    };
  }

  @Override
  public ForwardIndex getForwardIndex(final String column) {
    final ColumnMetadata columnMetadata = properties.get(columns.get(0));
    final int count = columnIndexes.size();
    return new SingleValueForwardIndex() {
      @Override
      public int getLength() {
        return columnMetadata.getNumberOfElements() / count;
      }
      @Override
      public TermValueList<?> getDictionary() {
        return dictionary;
      }
      @Override
      public ColumnType getColumnType() {
        return columnType;
      }
      @Override
      public SingleValueRandomReader getReader() {
        return CompositeMetricCustomIndex.this.getReader(column);
      }
    };
  }
  @Override
  public TermValueList getDictionary(String column) {
    return dictionary;
  }
  @Override
  public InvertedIndex getInvertedIndex(String column) {
    return null;
  }

  @Override
  public ColumnType getColumnType(String column) {
    return columnType;
  }
}
