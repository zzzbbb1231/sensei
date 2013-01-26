package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.concurrent.locks.ReadWriteLock;

import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;

public class LongDictionarySnapshot extends AbstractDictionarySnapshot {

  private LongList unsortedValues;
  
  private static ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    final String format = DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG);

    protected DecimalFormat initialValue() {
      return new DecimalFormat(format);
    }
  };
 
  public void init(Long2IntMap map, ReadWriteLock lock) {
    if (unsortedValues != null && unsortedValues.size() == map.size()) {
      // do nothing
      return;
    }
    try {
      lock.readLock().lock();
      if (unsortedValues == null) {
        unsortedValues = new LongArrayList(map.size());
      }
       int previousSize = unsortedValues.size();
        unsortedValues.size(map.size());
        for (it.unimi.dsi.fastutil.longs.Long2IntMap.Entry entry : map.long2IntEntrySet()) {
          if (entry.getIntValue() >= previousSize) {
            unsortedValues.set(entry.getIntValue(), entry.getLongKey());
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
        long val1 = unsortedValues.get(permutationArray.getInt(k1));
        long val2 = unsortedValues.get(permutationArray.getInt(k2));
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

  private long getValue(int unsortedDictId) {
    if (unsortedDictId < 2) {
      return 0;
    }
    return unsortedValues.getLong(unsortedDictId);
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
    return getValue(unsortedDictId);
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
    final long[] longArr = new long[unsortedValues.size() - 1];
    for (int i = 0; i < unsortedValues.size(); i++) {
      Integer index = permutationArray.get(i);
      if (index == 0) {
        continue;
      }
      if (index == 1) {
        longArr[0] = TermLongList.VALUE_MISSING;
        continue;
      }
      longArr[index - 1] = unsortedValues.getLong(i);
    }
    TermLongList termLongList =
        new TermLongList(unsortedValues.size(), DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG)) {
          public int size() {
            return longArr.length;
          }
          @Override
          public Long getRawValue(int index) {
            return super.getPrimitiveValue(index);
          }
        };
    Field longField;
   
      longField = TermLongList.class.getDeclaredField("_elements");
    
    longField.setAccessible(true);
    longField.set(termLongList, longArr);
    return termLongList;
    } catch (Exception e) {
     throw new RuntimeException(e);
    }
  }


  @Override
  public ComparableToInt comparableValue(String value) {   
    final long val1 = Long.parseLong(value);    
    return new ComparableToInt() {      
      @Override
      public int compareTo(int index) {
        long val2 = unsortedValues.getLong(permutationArray.getInt(index));
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
