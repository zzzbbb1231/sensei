package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;

public class FloatDictionarySnapshot extends AbstractDictionarySnapshot {

  private FloatList unsortedValues;
  
  private static ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    final String format = DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.FLOAT);

    protected DecimalFormat initialValue() {
      return new DecimalFormat(format);
    }
  };
 
  public void init(Float2IntMap map, ReadWriteLock lock) {
    if (unsortedValues != null && unsortedValues.size() == map.size()) {
      // do nothing
      return;
    }
    try {
      lock.readLock().lock();
      if (unsortedValues == null) {
        unsortedValues = new FloatArrayList(map.size());
      }
       int previousSize = unsortedValues.size();
        unsortedValues.size(map.size());
        for (it.unimi.dsi.fastutil.floats.Float2IntMap.Entry entry : map.float2IntEntrySet()) {
          if (entry.getIntValue() >= previousSize) {
            unsortedValues.set(entry.getIntValue(), entry.getFloatKey());
          }
        }
      
    } finally {
      lock.readLock().unlock();
    }
    if (permutationArray == null) {
      permutationArray = new IntArrayList(map.size());
    }
      permutationArray.clear();
      permutationArray.size(map.size());
      for (int i = 0; i < map.size(); i++) {
        permutationArray.set(i, i);
      }
   
    it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.size(), new IntComparator() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }

      @Override
      public int compare(int k1, int k2) {
        float val1 = unsortedValues.get(permutationArray.getInt(k1));
        float val2 = unsortedValues.get(permutationArray.getInt(k2));
        if (val1 > val2)
          return 1;
        if (val1 < val2)
          return -1;
        return 0;
      }
    }, new Swapper() {
      @Override
      public void swap(int a, int b) {
        int tmp = permutationArray.getInt(b);
        permutationArray.set(b, permutationArray.get(a));
        permutationArray.set(a, tmp);
      }
    });
  }

 
  @Override
  public String getStringValue(int unsortedDictId) {
    return formatter.get().format(getValue(unsortedDictId));
  }

  private float getValue(int unsortedDictId) {
    if (unsortedDictId < 2) {
      return 0;
    }
    return unsortedValues.getFloat(unsortedDictId);
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getObject(int)
   */
  @Override
  public Object getObject(int unsortedDictId) {
    return  getValue(unsortedDictId);
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getInt(int)
   */
  @Override
  public int getIntValue(int unsortedDictId) {
    return (int) getValue(unsortedDictId);
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getLong(int)
   */
  @Override
  public long getLongValue(int unsortedDictId) {
    return (long) getValue(unsortedDictId);
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getDouble(int)
   */
  @Override
  public double getDoubleValue(int unsortedDictId) {
    return getValue(unsortedDictId);
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getFloat(int)
   */
  @Override
  public float getFloatValue(int unsortedDictId) {
    return getValue(unsortedDictId);
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getShort(int)
   */
  @Override
  public short getShortValue(int unsortedDictId) {
    return (short) getValue(unsortedDictId);
  }


  @Override
  public void recycle() {
    permutationArray.clear();
    unsortedValues.clear();
    
  }


  @Override
  public TermValueList produceDictionary() {
    try {
    final float[] floatArr = new float[unsortedValues.size() - 1];
    for (int i = 0; i < unsortedValues.size(); i++) {
      Integer index = permutationArray.get(i);
      if (index == 0) {
        continue;
      }
      if (index == 1) {
        floatArr[0] = TermFloatList.VALUE_MISSING;
        continue;
      }
      floatArr[index - 1] = unsortedValues.getFloat(i);
    }
    TermFloatList termFloatList =
        new TermFloatList(unsortedValues.size(), DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.FLOAT)) {
          public int size() {
            return floatArr.length;
          }
          @Override
          public Float getRawValue(int index) {
            return super.getPrimitiveValue(index);
          }
        };
    Field floatField;
   
      floatField = TermFloatList.class.getDeclaredField("_elements");
    
      floatField.setAccessible(true);
      floatField.set(termFloatList, floatArr);
    return termFloatList;
    } catch (Exception e) {
     throw new RuntimeException(e);
    }
  }


  @Override
  public ComparableToInt comparableValue(String value) {   
    final float val1 = Float.parseFloat(value);    
    return new ComparableToInt() {      
      @Override
      public int compareTo(int index) {
        float val2 = unsortedValues.getFloat(permutationArray.getInt(index));
        if (val1 < val2) {
          return 1;
        }
        if (val1 > val2) {
          return -1;
        }
        return 0;
      }
    };
  }

  @Override
  public String format(Object o) {
    
    return formatter.get().format(o);
  }
 




}
