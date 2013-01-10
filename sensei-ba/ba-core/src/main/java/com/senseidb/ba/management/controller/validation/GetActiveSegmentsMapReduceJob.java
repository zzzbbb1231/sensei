package com.senseidb.ba.management.controller.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.mapred.BaFieldAccessor;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.util.JSONUtil;

public class GetActiveSegmentsMapReduceJob implements SenseiMapReduce<String, ArrayList<String>>{

  @Override
  public void init(JSONObject params) {
  }

  @Override
  public String map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    if (!(facetCountsAccessor == FacetCountAccessor.EMPTY)) {
      BoboIndexReader boboIndexReader = ((BaFieldAccessor) accessor).getBoboIndexReader();
      SegmentToZoieReaderAdapter adapter =  (SegmentToZoieReaderAdapter) boboIndexReader.getInnerReader();
      return adapter.getSegmentId();
    }
    return null;
  }

  @Override
  public List<String> combine(List<String> mapResults, CombinerStage combinerStage) {
    return mapResults;
  }

  @Override
  public ArrayList<String> reduce(List<String> combineResults) {
    return new ArrayList<String>(combineResults);
  }

  @Override
  public JSONObject render(ArrayList<String> reduceResult) {
    Collections.sort(reduceResult);
    try {
      return new JSONUtil.FastJSONObject().put("result", new JSONUtil.FastJSONArray(reduceResult));
    } catch (JSONException e) {
     throw new RuntimeException(e);
    }
  }

  
    
}
