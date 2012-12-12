package com.senseidb.search.req.mapred.functions.groupby;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.util.JSONUtil;

public class AggregateFunctionFactory {
    public static AggregateFunction valueOf(String name, String column) {
        name = name.toLowerCase();
        if (name.endsWith("avg")) {
            return new AvgAggregationFunction(column);
        }
        if (name.endsWith("sum")) {
            return new SumAggregationFunction(column);
        }
        if (name.endsWith("count")) {
          return new CountAggregationFunction(column);
      }
        if (name.endsWith("max")) {
          return new MaxAggregationFunction(column);
      }
        if (name.endsWith("min")) {
          return new MinAggregationFunction(column);
      }
        return null;
    }
    public static List<String> sort(final Map<String, ? extends GroupedValue> reduceResult) {
      List<String> ret = new ArrayList<String>(reduceResult.keySet());
      Collections.sort(ret, new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
          return reduceResult.get(o1).compareTo(reduceResult.get(o2));
        }
      });
      return ret;
    }
    public static class SumGroupedValue implements GroupedValue {
        long sum = 0;

        @Override
        public int compareTo(GroupedValue o) {
            long val = ((SumGroupedValue) o).sum - sum;
            if (val < 0)
                return -1;
            if (val == 0)
                return 0;
            return 1;
        }

        @Override
        public void merge(GroupedValue anotherValue) {
            sum += ((SumGroupedValue) anotherValue).sum;
        }

    }

    public static class SumAggregationFunction implements AggregateFunction<SumGroupedValue> {
        private final String column;

        public SumAggregationFunction(String column) {
            this.column = column;
        }

        @Override
        public SumGroupedValue produceSingleValue(FieldAccessor accessor, int docId) {
            SumGroupedValue ret = new SumGroupedValue();
            ret.sum = accessor.getLong(column, docId);
            return ret;
        }

        public Object toJson(HashMap<String, SumGroupedValue> reduceResult) {
          try {
              JSONArray ret = new JSONUtil.FastJSONArray();
              for (String key : AggregateFunctionFactory.sort(reduceResult)) {
                SumGroupedValue value = reduceResult.get(key);
                  ret.put(new JSONUtil.FastJSONObject().put("sum", value.sum).put("group", key));
              }
              return ret;
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
      }

    }
    public static class CountGroupedValue implements GroupedValue {
      long count = 0;

      @Override
      public int compareTo(GroupedValue o) {
          long val = ((CountGroupedValue) o).count - count;
          if (val < 0)
              return -1;
          if (val == 0)
              return 0;
          return 1;
      }

      @Override
      public void merge(GroupedValue anotherValue) {
        count += ((CountGroupedValue) anotherValue).count;
      }

  }

  public static class CountAggregationFunction implements AggregateFunction<CountGroupedValue> {
      private final String column;

      public CountAggregationFunction(String column) {
          this.column = column;
      }

      @Override
      public CountGroupedValue produceSingleValue(FieldAccessor accessor, int docId) {
        CountGroupedValue ret = new CountGroupedValue();
          ret.count = 1;
          return ret;
      }
      public Object toJson(HashMap<String, CountGroupedValue> reduceResult) {
        try {
            JSONArray ret = new JSONUtil.FastJSONArray();
            for (String key : AggregateFunctionFactory.sort(reduceResult)) {
              CountGroupedValue value = reduceResult.get(key);
                ret.put(new JSONUtil.FastJSONObject().put("count", value.count).put("group", key));
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
      

  }

    public static class AvgAggregationFunction implements AggregateFunction<AvgGroupedValue> {
        private final String column;

        public AvgAggregationFunction(String column) {
            this.column = column;
        }

        @Override
        public AvgGroupedValue produceSingleValue(FieldAccessor accessor, int docId) {
            AvgGroupedValue ret = new AvgGroupedValue();
            ret.avg = accessor.getDouble(column, docId);
            ret.count = 1;
            return ret;
        }
        public Object toJson(HashMap<String, AvgGroupedValue> reduceResult) {
          try {
              JSONArray ret = new JSONUtil.FastJSONArray();
              for (String key : AggregateFunctionFactory.sort(reduceResult)) {
                AvgGroupedValue value = reduceResult.get(key);
                  ret.put(new JSONUtil.FastJSONObject().put("avg",  String.format("%1.5f", value.avg)).put("group", key));
              }
              return ret;
          } catch (Exception e) {
              throw new RuntimeException(e);
          }
      }
       

    }
    public static class AvgGroupedValue implements GroupedValue {
      double avg = 0;
      int count = 0;
      @Override
      public int compareTo(GroupedValue o) {
          double val = ((AvgGroupedValue) o).avg - avg;
          if (val < 0)
              return -1;
          if (val == 0)
              return 0;
          return 1;
      }

      @Override
      public void merge(GroupedValue anotherValue) {
          AvgGroupedValue anValue = ((AvgGroupedValue) anotherValue);
          int newCount = count + anValue.count;
          avg = (avg * count + anValue.avg * anValue.count) / newCount;
          count = newCount;
      }

  }

  public static class MaxAggregationFunction implements AggregateFunction<MaxGroupedValue> {
      private final String column;

      public MaxAggregationFunction(String column) {
          this.column = column;
      }

      @Override
      public MaxGroupedValue produceSingleValue(FieldAccessor accessor, int docId) {
        MaxGroupedValue ret = new MaxGroupedValue();
          ret.max = accessor.getDouble(column, docId);
          ret.uid = docId;
          return ret;
      }
      public Object toJson(HashMap<String, MaxGroupedValue> reduceResult) {
        try {
            JSONArray ret = new JSONUtil.FastJSONArray();
            for (String key : AggregateFunctionFactory.sort(reduceResult)) {
              MaxGroupedValue value = reduceResult.get(key);
                ret.put(new JSONUtil.FastJSONObject().put("max",  String.format("%1.5f", value.max)).put("group", key));
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
     

  }
  public static class MaxGroupedValue implements GroupedValue {
    double max = 0;
    long uid;
    @Override
    public int compareTo(GroupedValue o) {
        double val = ((MaxGroupedValue) o).max - max;
        if (val < 0)
            return -1;
        if (val == 0)
            return 0;
        return 1;
    }

    @Override
    public void merge(GroupedValue anotherValue) {
      MaxGroupedValue anValue = ((MaxGroupedValue) anotherValue);
        if (anValue.max > max) {
          max = anValue.max;
          uid = anValue.uid;
        }
       
    }

}
  
  
  public static class MinAggregationFunction implements AggregateFunction<MinGroupedValue> {
    private final String column;

    public MinAggregationFunction(String column) {
        this.column = column;
    }

    @Override
    public MinGroupedValue produceSingleValue(FieldAccessor accessor, int docId) {
      MinGroupedValue ret = new MinGroupedValue();
        ret.min = accessor.getDouble(column, docId);
        ret.uid = docId;
        return ret;
    }

    @Override
    public Object toJson(HashMap<String, MinGroupedValue> reduceResult) {
        try {
            JSONArray ret = new JSONUtil.FastJSONArray();
            for (String key : AggregateFunctionFactory.sort(reduceResult)) {
              MinGroupedValue value = reduceResult.get(key);
                ret.put(new JSONUtil.FastJSONObject().put("min",  String.format("%1.5f", value.min)).put("group", key));
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
public static class MinGroupedValue implements GroupedValue {
  double min = 0;
  long uid;
  @Override
  public int compareTo(GroupedValue o) {
      double val = ((MinGroupedValue) o).min - min;
      val = val * -1;
      if (val < 0)
          return -1;
      if (val == 0)
          return 0;
      return 1;
  }

  @Override
  public void merge(GroupedValue anotherValue) {
    MinGroupedValue anValue = ((MinGroupedValue) anotherValue);
      if (anValue.min < min) {
        min = anValue.min;
        uid = anValue.uid;
      }
     
  }

}
}

