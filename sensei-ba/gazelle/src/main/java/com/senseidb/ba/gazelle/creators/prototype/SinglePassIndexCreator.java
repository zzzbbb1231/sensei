package com.senseidb.ba.gazelle.creators.prototype;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigNestedIntArray;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.BufferedLoader;

public class SinglePassIndexCreator {
    private Float2IntOpenHashMap float2IntMap;
    private Long2IntOpenHashMap long2IntMap;
    private Object2IntOpenHashMap<String> string2IntMap;
    int counter = 1;
    private IntBuffer intBuffer;
    private BufferedLoader bufferedLoader;
    private final int size;
    private int maxIndex = -1;
    private int[] permutationArray;
    private BigNestedIntArray bigNestedIntArray;
    private int maxValuesCount = -1;
    public SinglePassIndexCreator(int size) {
      this.size = size;
    }
    
    public void addPrimitiveValue(int index, Object value) {
     
      int indexToAdd = 0;
      if (value instanceof String) {
        if (string2IntMap == null) {
          string2IntMap = new Object2IntOpenHashMap<String>(10000);
        }
        if (string2IntMap.containsKey(value)) {
          indexToAdd = string2IntMap.get(value);
        } else {
          indexToAdd = counter++;
          string2IntMap.put((String)value, indexToAdd);
        }     
        
      } else 
      if (value instanceof Long || value instanceof Integer) {
        if (long2IntMap == null) {
          long2IntMap = new Long2IntOpenHashMap(10000);
        }
        long longValue = ((Number)value).longValue();
        if (long2IntMap.containsKey(longValue)) {
          indexToAdd = long2IntMap.get(longValue);
        } else {
          indexToAdd = counter++;
          long2IntMap.put(longValue, indexToAdd);
        } 
      } else 
      if (value instanceof Float || value instanceof Double) {
        if (float2IntMap == null) {
          float2IntMap = new Float2IntOpenHashMap(10000);
        }
        float floatValue = ((Number)value).floatValue();
        if (float2IntMap.containsKey(floatValue)) {
          indexToAdd = float2IntMap.get(floatValue);
        } else {
          indexToAdd = counter++;
          float2IntMap.put(floatValue, indexToAdd);
        } 
      } else {
        throw new UnsupportedOperationException("" + value.getClass());
      }
      addToForwardIndex(index, indexToAdd);
    }
    public void addValue(int index, Object value) {
      if (value == null) {
        return;
      }
      if (value instanceof Object[]) {
        if (bufferedLoader == null) {
          bufferedLoader = new com.senseidb.ba.gazelle.utils.BufferedLoader(size) {
            @Override
            protected void add(int id, int[] data, int off, int len) {
              if (id > maxIndex) {
                return;
              }
              for (int i = off; i < len; i++) {
                data[i] = permutationArray[data[i]];
              }
                super.add(id, data, off, len);
            }
          };
          Assert.state(intBuffer == null);
        }
        Object[] values = (Object[]) value;
        if (values.length > maxValuesCount) {
          maxValuesCount = values.length;
        }
        for (Object val : values) {
          addPrimitiveValue(index, val);
        }
      } else {
        if (intBuffer == null) {
          intBuffer = ByteBuffer.allocateDirect(size * 4).asIntBuffer();
        }
        Assert.state(bufferedLoader == null);
        addPrimitiveValue(index, value);
      }
    }
    
    public void addToForwardIndex(int index, int indexToAdd) {
      if (maxIndex < index) {
        maxIndex = index;
      }
      if (intBuffer != null) {
        intBuffer.put(index, indexToAdd);
      }
      else {
        bufferedLoader.add(index, indexToAdd);
      }
    }
    private TermValueList prepareLongDictionary() throws Exception {
      final long[] arr = new long[long2IntMap.size() + 1];
      LongIterator iterator = long2IntMap.keySet().iterator();
      
      while(iterator.hasNext()) {
        long nextLong = iterator.nextLong();
        arr[long2IntMap.get(nextLong)] = nextLong;
      }
      permutationArray = new int[long2IntMap.size()];
      for (int j = 0; j < permutationArray.length; j++) {
        permutationArray[j] = j + 1;
      }
      it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.length, new IntComparator() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return compare(o1.intValue(), o2.intValue());
        }
        @Override
        public int compare(int k1, int k2) {
          if (arr[permutationArray[k1]] > arr[permutationArray[k2]]) return 1;
          if (arr[permutationArray[k2]] > arr[permutationArray[k1]]) return -1;
          return 0;
        }
      }, new Swapper() {
        @Override
        public void swap(int a, int b) {
          int tmp = permutationArray[b];
          permutationArray[b] = permutationArray[a];
          permutationArray[a] = tmp;
        }
      });
      Arrays.sort(arr, 1, arr.length);
      resortForwardIndex(); 
      return produceDictionary(arr);
   }
    private TermValueList prepareFloatDictionary() throws Exception {
      final float[] arr = new float[float2IntMap.size() + 1];
      FloatIterator iterator = float2IntMap.keySet().iterator();
      
      while(iterator.hasNext()) {
        float nextFloat = iterator.nextFloat();
        arr[float2IntMap.get(nextFloat)] = nextFloat;
      }
      permutationArray = new int[long2IntMap.size()];
      for (int j = 0; j < permutationArray.length; j++) {
        permutationArray[j] = j + 1;
      }
      it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.length, new IntComparator() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return compare(o1.intValue(), o2.intValue());
        }
        @Override
        public int compare(int k1, int k2) {
          if (arr[k1] > arr[k2]) return 1;
          if (arr[k2] > arr[k1]) return -1;
          return 0;
        }
      }, new Swapper() {
        @Override
        public void swap(int a, int b) {
          int tmp = permutationArray[b];
          permutationArray[b] = permutationArray[a];
          permutationArray[a] = tmp;
        }
      });
      Arrays.sort(arr, 1, arr.length);
      resortForwardIndex(); 
      return produceDictionary(arr);
   }
    private TermValueList prepareStringDictionary() throws Exception {
      final TermStringList arr = new TermStringList(string2IntMap.size() + 1);
      ObjectIterator<String> iterator = string2IntMap.keySet().iterator();
      final List<String> innerList = (List<String>)arr.getInnerList();
      
      while(iterator.hasNext()) {
        String str = iterator.next();
        innerList.set(string2IntMap.get(str), str);
      }
      permutationArray = new int[string2IntMap.size()];
      for (int j = 0; j < permutationArray.length; j++) {
        permutationArray[j] = j + 1;
      }
      it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.length, new IntComparator() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return compare(o1.intValue(), o2.intValue());
        }
        @Override
        public int compare(int k1, int k2) {
          return innerList.get(k1).compareTo(innerList.get(k2));
         
        }
      }, new Swapper() {
        @Override
        public void swap(int a, int b) {
          int tmp = permutationArray[b];
          permutationArray[b] = permutationArray[a];
          permutationArray[a] = tmp;
        }
      });
      Collections.sort(innerList.subList(0, innerList.size()));
      arr.seal();
      return arr;
   }
    private TermValueList produceDictionary(String[] arr) {
      // TODO Auto-generated method stub
      return null;
    }
    private TermValueList produceDictionary(final float[] arr) throws Exception {
      TermFloatList termFloatList =
          new TermFloatList(arr.length, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG)) {
            public int size() {
              return arr.length;
            }
          };
      Field floatField = TermFloatList.class.getDeclaredField("_elements");
      floatField.setAccessible(true);
      floatField.set(termFloatList, arr);
      return termFloatList;
    }
    public TermValueList produceDictionary(final long[] arr) throws Exception {
      if (arr[arr.length - 1] <= Integer.MAX_VALUE) {
        int[] shortArray = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
          shortArray[i] = (int) arr[i];
        }
        TermIntList termIntList =
            new TermIntList(arr.length, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG)) {
              public int size() {
                return arr.length;
              }
            };
        Field intField = TermIntList.class.getDeclaredField("_elements");
        intField.setAccessible(true);
        intField.set(termIntList, shortArray);
        return termIntList;
      }
      TermLongList termLongList =
          new TermLongList(arr.length, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG)) {
            public int size() {
              return arr.length;
            }
          };
      Field intField = TermIntList.class.getDeclaredField("_elements");
      intField.setAccessible(true);
      intField.set(termLongList, arr);
      return termLongList;
    }
    public void resortForwardIndex() throws Exception {
      if (intBuffer != null) {
        for (int i = 0; i < maxIndex + 1; i++) {
          intBuffer.put(i, permutationArray[intBuffer.get(i)]);
        }
      }
     else {
      
       bigNestedIntArray = new BigNestedIntArray();
       bigNestedIntArray.load(maxIndex + 1, bufferedLoader);       
    }
    }
    public TermValueList seal() throws Exception {
      if (string2IntMap != null) {
        return prepareStringDictionary();
      } 
      if (float2IntMap != null) {
        return prepareFloatDictionary();
      }
      if (long2IntMap != null) {
        return prepareLongDictionary();
      }
      throw new UnsupportedOperationException();
    }
    public boolean isSingleValue() {
      return intBuffer != null;
    }
    public boolean isMulti() {
      return !isSingleValue();
    }
    public int getValueIndex(int index) {
      Assert.state(isSingleValue());
      return intBuffer.get(index);
    }
    public int getValueIndexes(int id, int[] buffer) {
      Assert.state(isSingleValue());
      return bigNestedIntArray.getData(id, buffer);
    }

    public int getMaxValuesCount() {
      return maxValuesCount;
    }
}
