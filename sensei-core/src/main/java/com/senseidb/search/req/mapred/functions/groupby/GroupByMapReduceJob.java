package com.senseidb.search.req.mapred.functions.groupby;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.functions.groupby.AggregateFunction.GroupedValue;

public class GroupByMapReduceJob implements SenseiMapReduce<HashMap<String, GroupedValue<Serializable>>, HashMap<String, GroupedValue<Serializable>>> {

    private static final int TRIM_SIZE = 1000;
    private String[] columns;
    private String metric;
    private String function;
    private AggregateFunction aggregateFunction;
    private HashMap<String, GroupedValue<Serializable>> map = new HashMap<String, GroupedValue<Serializable>>();;
    @Override
    public void init(JSONObject params) {
      try {
        metric = params.getString("metric");
        function = params.getString("function");
        aggregateFunction = AggregateFunctionFactory.valueOf(function);
        JSONArray columnsJson = params.getJSONArray("columns");
        columns = new String[columnsJson.length()];
        for (int i = 0; i < columnsJson.length(); i++) {
          columns[i] = columnsJson.getString(i);
        }
      } catch (JSONException ex) {
        throw new RuntimeException(ex);
      }
      
    }

    @Override
    public HashMap<String, GroupedValue<Serializable>> map(int[] docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
      for (int i =0; i < docIdCount; i++) {
        String key = getKey(columns, accessor, i);
        GroupedValue<Serializable> value = map.get(key);
        
        if (value != null) {
          value.merge(aggregateFunction.produceSingleValue(accessor, i));
        } else {
            map.put(key, aggregateFunction.produceSingleValue(accessor, i));
        }
      }
      trimToSize(map, TRIM_SIZE);
      return null;
    }

    private void trimToSize(HashMap<String, GroupedValue<Serializable>> map, int count) {
        if (map.size() < count) {
            return;
        }
        double trimRatio = ((double) count) / map.size();
        int queueSize = (int)(map.size() / Math.log(map.size())) / 2;
        PriorityQueue<GroupedValue<Serializable>> queue = new PriorityQueue<GroupedValue<Serializable>>(queueSize);
        int i = 0;
        int addElementRange = map.size() / queueSize;
        for (GroupedValue<Serializable> groupedValue : map.values()) {
            if (i == addElementRange) {
                i = 0;
                queue.add(groupedValue);
            } else {
                i++;
            }
        }
        int elementIndex = (int) (queue.size() * trimRatio);
        Iterator<GroupedValue<Serializable>> iterator =  queue.iterator();
        int counter = 0;
        GroupedValue<Serializable> newMinimumValue = null;
        while(iterator.hasNext()) {
            if (counter == elementIndex) {
                newMinimumValue =  iterator.next();
                break;
            } else {
                counter++;
                iterator.next();
            }
        }
        if (newMinimumValue == null) {
            return;
        }
        iterator = map.values().iterator();
        int numToRemove = map.size() - count;
       counter = 0;
       while (iterator.hasNext()) {
            if (iterator.next().compareTo((Serializable) newMinimumValue) <= 0) {
                counter++;
                iterator.remove();
                 if (counter >= numToRemove) {
                 break;}
            }
        }
        
    }

    @Override
    public List<HashMap<String, GroupedValue<Serializable>>> combine(List<HashMap<String, GroupedValue<Serializable>>> mapResults, CombinerStage combinerStage) {
      if (combinerStage == CombinerStage.partitionLevel) {
          List<HashMap<String, GroupedValue<Serializable>>> ret = java.util.Arrays.asList(map);
          map = null;
          return ret;
      } else {
          if (mapResults.size() < 2) {
              return mapResults;
          }
          HashMap<String, GroupedValue<Serializable>> firstMap = mapResults.get(0);
          for (int i = 1; i < mapResults.size() ; i++) {
              merge(firstMap, mapResults.get(i));
          }
          trimToSize(firstMap, 2 * TRIM_SIZE);
          return java.util.Arrays.asList(firstMap);
      }
    }

    private void merge(HashMap<String, GroupedValue<Serializable>> firstMap, HashMap<String, GroupedValue<Serializable>> secondMap) {
        for(Map.Entry<String, GroupedValue<Serializable>> entry : secondMap.entrySet()) {
            GroupedValue<Serializable> groupedValue = firstMap.get(entry.getKey());
            if (groupedValue != null) {
                groupedValue.merge(entry.getValue());
            } else {
                firstMap.put(entry.getKey(), entry.getValue());
            }
        }
        
    }

    @Override
    public HashMap<String, GroupedValue<Serializable>> reduce(List<HashMap<String, GroupedValue<Serializable>>> combineResults) {
        if (combineResults.size() == 0) {
            return null;
        }
        if (combineResults.size() == 1) {
            return combineResults.get(0);
        }
        HashMap<String, GroupedValue<Serializable>> firstMap = combineResults.get(0);
        for (int i = 1; i < combineResults.size() ; i++) {
            merge(firstMap, combineResults.get(i));
        }
        trimToSize(firstMap, TRIM_SIZE);
        return firstMap;
    }

    @Override
    public JSONObject render(HashMap<String, GroupedValue<Serializable>> reduceResult) {
      return aggregateFunction.toJson(reduceResult);
    }
   
    private String getKey(String[] columns, FieldAccessor fieldAccessor, int docId) {
      StringBuilder key = new StringBuilder(fieldAccessor.get(columns[0], docId).toString());
      for (int i = 1; i < columns.length; i++) {
        key.append(":").append(fieldAccessor.get(columns[i], docId).toString());
      }
      return key.toString();
    }
  }