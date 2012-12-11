package com.senseidb.search.req.mapred.functions.groupby;


import java.util.HashMap;

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

        @Override
        public JSONObject toJson(HashMap<String, SumGroupedValue> reduceResult) {
            try {
                JSONObject ret = new JSONUtil.FastJSONObject();
                for (String key : reduceResult.keySet()) {
                    SumGroupedValue value = (SumGroupedValue) reduceResult.get(key);
                    ret.put(key, new JSONUtil.FastJSONObject().put("sum", value.sum));
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
          ret.count = accessor.getLong(column, docId);
          return ret;
      }

      @Override
      public JSONObject toJson(HashMap<String, CountGroupedValue> reduceResult) {
          try {
              JSONObject ret = new JSONUtil.FastJSONObject();
              for (String key : reduceResult.keySet()) {
                CountGroupedValue value = (CountGroupedValue) reduceResult.get(key);
                  ret.put(key, new JSONUtil.FastJSONObject().put("count", value.count));
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

        @Override
        public JSONObject toJson(HashMap<String, AvgGroupedValue> reduceResult) {
            try {
                JSONObject ret = new JSONUtil.FastJSONObject();
                for (String key : reduceResult.keySet()) {
                    AvgGroupedValue value = reduceResult.get(key);
                    ret.put(key, new JSONUtil.FastJSONObject().put("avg", value.avg));
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

      @Override
      public JSONObject toJson(HashMap<String, MaxGroupedValue> reduceResult) {
          try {
              JSONObject ret = new JSONUtil.FastJSONObject();
              for (String key : reduceResult.keySet()) {
                MaxGroupedValue value = reduceResult.get(key);
                  ret.put(key, new JSONUtil.FastJSONObject().put("max", value.max));
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
    public JSONObject toJson(HashMap<String, MinGroupedValue> reduceResult) {
        try {
            JSONObject ret = new JSONUtil.FastJSONObject();
            for (String key : reduceResult.keySet()) {
              MinGroupedValue value = reduceResult.get(key);
                ret.put(key, new JSONUtil.FastJSONObject().put("min", value.min));
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

