package com.senseidb.ba.mapred.impl;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.ba.gazelle.utils.SortUtil;
import com.senseidb.search.req.mapred.CombinerStage;
import com.senseidb.search.req.mapred.FacetCountAccessor;
import com.senseidb.search.req.mapred.FieldAccessor;
import com.senseidb.search.req.mapred.IntArray;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.util.JSONUtil;
 class MapResult implements Serializable {
  public MapResult() {
 }
  
  public MapResult(long[] vals, int count) {
   super();
   this.vals = vals;
   this.count = count;
 }

  long[] vals;
  int count;
  
  int currentStart = 0;
}
public class SortedUniqueValues implements SenseiMapReduce<MapResult, MapResult> {
  private String valueColumn;
  private String sortColumn;  
  private int offset = 0;
  private int size = 100;
  
  @Override
  public void init(JSONObject params) {
    try {
      valueColumn = params.getString("valueColumn");
      sortColumn = params.getString("sortColumn");
      offset = params.optInt("offset", 0);
      size = params.optInt("size", 10);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    
  }
 
  @Override
  public MapResult map(IntArray docIds, int docIdCount, long[] uids, FieldAccessor accessor, FacetCountAccessor facetCountsAccessor) {
    long[] valuesWithSorts = new long[docIdCount];
    SingleFieldAccessor valueAccessor = accessor.getSingleFieldAccessor(valueColumn);
    SingleFieldAccessor sortAccessor = accessor.getSingleFieldAccessor(sortColumn);
    for (int i = 0; i < docIdCount; i++) {
      long value = valueAccessor.getInteger(docIds.get(i));
      value <<= 32;
      value = value | sortAccessor.getInteger(docIds.get(i));
      valuesWithSorts[i] = value;
    }
    java.util.Arrays.sort(valuesWithSorts);
    int size = removeDuplicates(valuesWithSorts);
    return new MapResult(valuesWithSorts, size);
  }

  public static int removeDuplicates(long[] valuesWithSorts) {
    int start = 0;
    for (int i = 1; i < valuesWithSorts.length; i++) {
      if (valuesWithSorts[start] >>> 32 == valuesWithSorts[i] >>> 32) {
        valuesWithSorts[start] = valuesWithSorts[i];
      } else {
        start++;
        if (start != i) {
          valuesWithSorts[start] = valuesWithSorts[i];
        }
      }
    }
    return start + 1;
  }

  @Override
  public List<MapResult> combine(List<MapResult> mapResults, CombinerStage combinerStage) {
    if (mapResults.size() <= 1) {
      return mapResults;
    }
    MapResult[] priorityQueue = new MapResult[mapResults.size()];
    int totalCount = 0;
    for (int i = 0; i < mapResults.size(); i++) {
      priorityQueue[i] = mapResults.get(i);
      totalCount += priorityQueue[i].count;
    }
    long[] arr = new long[totalCount];
    int arrIndex = merge(priorityQueue,  arr);
    if (arrIndex > size + offset) {
      trim(arr, arrIndex, size+offset);
      arrIndex = size+offset;
    }
    
    return  java.util.Arrays.asList(new MapResult(arr, arrIndex));
  }

  private static class ModifiedPriorityQueue extends IntHeapPriorityQueue {
    public ModifiedPriorityQueue(int capacity, IntComparator c) {
      super(capacity, c);
    }
    int[] getHeap() {
      return heap;
    }
  }
   static void trim(final long[] arr, int arrLength, int trimSize) {
    if (arrLength <= trimSize) {
      return;
    }
    IntHeapPriorityQueue priorityQueue = new IntHeapPriorityQueue(trimSize, new IntComparator() {      
      @Override
      public int compare(Integer o1, Integer o2) {
        throw new UnsupportedOperationException();
      }      
      @Override
      public int compare(int arg0, int arg1) {
        int time1 =  (int)(arr[arg0] & Integer.MAX_VALUE);
        int time2 =  (int)(arr[arg1] & Integer.MAX_VALUE);
        if (time1 > time2) return 1;
        if (time1 < time2) return -1;
        return 0;
      }
    });
    int size = 0;
    for (int i = 0; i < arrLength;i++) {
      if (size < trimSize) {
        priorityQueue.enqueue(i);
        size++;
      } else {
        int time1 =  (int)(arr[i] & Integer.MAX_VALUE);
        int time2 =  (int)(arr[priorityQueue.firstInt()] & Integer.MAX_VALUE);
        if (time1 > time2) {
          arr[priorityQueue.dequeueInt()] = -1;
          priorityQueue.enqueue(i);
        }
      }      
    }
    int start = 0;
    for (int i = 0; i < arrLength; i++) {
      if (arr[i] < 0) {
        start = i;
        break;
      }
    }
    for (int i = start + 1; i < arrLength; i++) {
      if (arr[i] != -1) {
       arr[start] = arr[i];
       start++;
      }
    }    
  }

  public int merge(MapResult[] priorityQueue, long[] arr) {
    int arrIndex = 0;
    int startIndex = 0;
    java.util.Arrays.sort(priorityQueue, new Comparator<MapResult>() {
      @Override
      public int compare(MapResult o1, MapResult o2) {
        if (o1.currentStart >=  o1.count) {
          if (o2.currentStart >=  o2.count) return 0;
          return -1;
        }
        if (o2.currentStart >=  o2.count) return 1;
        long val1 = o1.vals[o1.currentStart];
        long val2 = o2.vals[o2.currentStart];
        if (val1 < val2) {
          return -1;
        }
        if (val1 > val2) return 1;
        return 0;
      }      
    });
    while(startIndex < priorityQueue.length) {
      MapResult min = priorityQueue[startIndex];
      long val = min.vals[min.currentStart];
      //remove duplicates
      if (arrIndex == 0 || (arr[arrIndex - 1] >>> 32 != val >> 32)) { 
        arr[arrIndex] = val;
        arrIndex++;       
      } else {
        if (arrIndex > 0)arr[arrIndex - 1] = val;
      }
      min.currentStart++;
      if (min.currentStart >= min.count) {
        startIndex++;
      } else {
        //maintain the sorted order in the priorityQueue
        int currentIndex = startIndex;
        while(currentIndex < priorityQueue.length - 1) {
         if (priorityQueue[currentIndex].vals[priorityQueue[currentIndex].currentStart] > priorityQueue[currentIndex+1].vals[priorityQueue[currentIndex+1].currentStart]) {
           MapResult tmp = priorityQueue[currentIndex];
           priorityQueue[currentIndex] = priorityQueue[currentIndex + 1];
           priorityQueue[currentIndex + 1] = tmp;
           currentIndex++;
         } else break;        
        }
      }
    }
    return arrIndex;
  }

  @Override
  public MapResult reduce(List<MapResult> combineResults) {
    
    return combine(combineResults, null).get(0);
  }

  @Override
  public JSONObject render(final MapResult reduceResult)  {
    
    try {   
    final long[] vals = reduceResult.vals;
    SortUtil.quickSort(0, reduceResult.count, new SortUtil.IntComparator() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }
      @Override
      public int compare(int k1, int k2) {
        int val1 = (int) (vals[k1] & Integer.MAX_VALUE);
        int val2 = (int) (vals[k2] & Integer.MAX_VALUE);
        if (val1 < val2)
          return 1;
        if (val1 > val2)
          return -1;
        return 0;
      }
    }, new SortUtil.Swapper() {
      @Override
      public void swap(int a, int b) {
        long tmp = vals[b];
        vals[b] =  vals[a];
        vals[a] =  tmp;
      }
    });
    JSONObject result = new JSONUtil.FastJSONObject();
    result.put("valueColumn", valueColumn); 
    result.put("sortColumn", sortColumn);
    JSONArray array = new JSONUtil.FastJSONArray();
    result.put("result", array);
    for (int i = offset; i < Math.min(reduceResult.count, offset + size); i++) {
      JSONObject item = new JSONUtil.FastJSONObject();
      item.put("value", vals[i] >>> 32);
      item.put("sorted",(int)( vals[i] & Integer.MAX_VALUE));
      array.put(item);
    }
    return result;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
public static void main(String[] args) {
 /* long[] arr = new long[] {10,12,2,3,5,7,7};
  trim(arr, arr.length, 5);
  System.out.println(java.util.Arrays.toString(arr));*/
  List<MapResult> combine = new SortedUniqueValues().combine(java.util.Arrays.asList(new MapResult(new long[]{pair(1,2), pair(2,3), pair(3,4)}, 3), new MapResult(new long[]{pair(1,3), pair(2,2), pair(3,3)}, 3), new MapResult(new long[]{pair(0,3),pair(1,1), pair(2,1), pair(3,1)}, 4)), null);
  for (int i = 0; i < combine.get(0).count; i++) {
    System.out.println(unpair(combine.get(0).vals[i]));
  }
}
public static long pair(int p1, int p2) {
  long ret = p1;
  ret <<= 32;
  return ret | p2; 
}
public static String unpair(long number) {
  return "first = " + (number >>> 32) + ", second = " + (number & Integer.MAX_VALUE);
}
}
