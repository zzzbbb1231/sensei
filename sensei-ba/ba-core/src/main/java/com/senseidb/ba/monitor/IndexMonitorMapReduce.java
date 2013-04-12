package com.senseidb.ba.monitor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.HighCardinalityInvertedIndex;
import com.senseidb.ba.gazelle.impl.InvertedIndexStatistics;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.mapred.BaFieldAccessor;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil;

class MapResult implements Serializable {

  public Map<String, InvertedIndexStatistics> invertedIndexStatisticsMap = new HashMap<String, InvertedIndexStatistics>();
  public Map<String, String> sortedStrategyMap = new HashMap<String, String>();
  public String segmentName;
  public InvertedIndexStatistics highCardinalityInvertedIndexStatistics = new InvertedIndexStatistics();
  public InvertedIndexStatistics standardCardinalityInvertedIndexStatistics = new InvertedIndexStatistics();
  public InvertedIndexStatistics totalInvertedIndexStatistics = new InvertedIndexStatistics();

  public MapResult(String segmentName, GazelleIndexSegmentImpl indexSeg) {
    this.segmentName = segmentName;
    for (String columnName : indexSeg.getColumnMetadataMap().keySet()) {
      ForwardIndex forwardIndex = indexSeg.getForwardIndex(columnName);
      if (forwardIndex instanceof SortedForwardIndexImpl) {
        sortedStrategyMap.put(columnName, "Sorted");
      } else {
        if (forwardIndex instanceof SecondarySortedForwardIndexImpl) {
          sortedStrategyMap.put(columnName, "SecondarySorted");
        } else {
          sortedStrategyMap.put(columnName, "UnSorted");
        }
      }

      InvertedIndex invertedIndex = indexSeg.getInvertedIndex(columnName);
      if (invertedIndex != null) {
        invertedIndexStatisticsMap.put(columnName, invertedIndex.getIndexStatistics());
        if (invertedIndex instanceof HighCardinalityInvertedIndex) {
          highCardinalityInvertedIndexStatistics.incrementStatisticsCount(invertedIndex.getIndexStatistics());
        } else {
          standardCardinalityInvertedIndexStatistics.incrementStatisticsCount(invertedIndex.getIndexStatistics());
        }
        totalInvertedIndexStatistics.incrementStatisticsCount(invertedIndex.getIndexStatistics());
      } else {
        invertedIndexStatisticsMap.put(columnName, new InvertedIndexStatistics());
      }
    }
  }
}

public class IndexMonitorMapReduce implements SenseiMapReduce<Serializable, ArrayList<Serializable>> {
  @Override
  public void init(JSONObject params) {
  }

  @Override
  public Serializable map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    if (!(facetCountsAccessor == FacetCountAccessor.EMPTY)) {
      BoboIndexReader boboIndexReader = ((BaFieldAccessor) accessor).getBoboIndexReader();
      SegmentToZoieReaderAdapter adapter = (SegmentToZoieReaderAdapter) boboIndexReader.getInnerReader();
      MapResult mapResult = new MapResult(adapter.getSegmentName(), (GazelleIndexSegmentImpl) adapter.getOfflineSegment());
      return mapResult;
    }
    return null;
  }

  @Override
  public List<Serializable> combine(List<Serializable> mapResults, CombinerStage combinerStage) {
    return mapResults;
  }

  @Override
  public ArrayList<Serializable> reduce(List<Serializable> combineResults) {
    return new ArrayList<Serializable>(combineResults);
  }

  @Override
  public JSONObject render(ArrayList<Serializable> reduceResults) {
    try {
      JSONObject renderResult = new JSONUtil.FastJSONObject();
      JSONArray segmentArray = new JSONUtil.FastJSONArray();
      InvertedIndexStatistics totalInvertedIndexStatisticsByAllSegments = new InvertedIndexStatistics();
      for (Serializable reduceResult : reduceResults) {
        JSONObject segmentEntry = segmentEntryToJSONObject(reduceResult);
        segmentArray.put(segmentEntry);
        totalInvertedIndexStatisticsByAllSegments.incrementStatisticsCount(((MapResult) reduceResult).totalInvertedIndexStatistics);
      }
      renderResult.put("allSegments__indexMonitor", segmentArray);
      renderResult.put("allSegments_memoryConsumption_invertedIndex", totalInvertedIndexStatisticsByAllSegments.getCompressedSize());
      renderResult.put("allSegments_totalDocumentsCount_invertedIndex", totalInvertedIndexStatisticsByAllSegments.getDocCount());
      renderResult.put("allSegments_documentsCount_invertedIndex", totalInvertedIndexStatisticsByAllSegments.getTrueDocCount());

      return renderResult;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private JSONObject segmentEntryToJSONObject(Serializable reduceResult) {
    Map<String, InvertedIndexStatistics> invertedIndexStatisticsMap = ((MapResult) reduceResult).invertedIndexStatisticsMap;
    Map<String, String> sortedStrategyMap = ((MapResult) reduceResult).sortedStrategyMap;
    InvertedIndexStatistics highCardinalityInvertedIndexStatistics = ((MapResult) reduceResult).highCardinalityInvertedIndexStatistics;
    InvertedIndexStatistics standardCardinalityInvertedIndexStatistics = ((MapResult) reduceResult).standardCardinalityInvertedIndexStatistics;
    InvertedIndexStatistics totalInvertedIndexStatistics = ((MapResult) reduceResult).totalInvertedIndexStatistics;
    JSONArray indexArray = new JSONUtil.FastJSONArray();
    for (String columnName : invertedIndexStatisticsMap.keySet()) {
      try {
        JSONObject indexEntry = new JSONUtil.FastJSONObject();
        indexEntry.put("column_name", columnName);
        indexEntry.put("column_sortedStrategy", sortedStrategyMap.get(columnName));
        indexEntry.put("column_invertedIndexStrategy", invertedIndexStatisticsMap.get(columnName).getInvertedIndexStrategy());
        indexEntry.put("column_memoryConsumption_invertedIndex", invertedIndexStatisticsMap.get(columnName).getCompressedSize());
        indexEntry.put("column_documentsCount_invertedIndex", invertedIndexStatisticsMap.get(columnName).getTrueDocCount());
        indexEntry.put("column_totalDocumentsCount_invertedIndex", invertedIndexStatisticsMap.get(columnName).getDocCount());
        indexArray.put(indexEntry);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      JSONObject segmentEntry = new JSONUtil.FastJSONObject();
      segmentEntry.put("segment_name", ((MapResult) reduceResult).segmentName);
      segmentEntry.put("segment__indexMonitor", indexArray);
      segmentEntry.put("segment_memoryConsumption_invertedIndex_total", totalInvertedIndexStatistics.getCompressedSize());
      segmentEntry.put("segment_memoryConsumption_invertedIndex_standardCardinality", standardCardinalityInvertedIndexStatistics.getCompressedSize());
      segmentEntry.put("segment_totalDocumentsCount_invertedIndex_standardCardinality", standardCardinalityInvertedIndexStatistics.getDocCount());
      segmentEntry.put("segment_documentsCount_invertedIndex_standardCardinality", standardCardinalityInvertedIndexStatistics.getTrueDocCount());
      segmentEntry.put("segment_memoryConsumption_invertedIndex_highCardinality", highCardinalityInvertedIndexStatistics.getCompressedSize());
      segmentEntry.put("segment_totalDocumentsCount_invertedIndex_highCardinality", highCardinalityInvertedIndexStatistics.getDocCount());
      segmentEntry.put("segment_documentsCount_invertedIndex_highCardinality", highCardinalityInvertedIndexStatistics.getTrueDocCount());
      return segmentEntry;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

}
