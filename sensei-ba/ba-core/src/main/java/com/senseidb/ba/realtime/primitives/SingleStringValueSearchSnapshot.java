package com.senseidb.ba.realtime.primitives;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.realtime.AbstractSearchSnapshot;
import com.senseidb.ba.realtime.ColumnSearchSnapshot;

public class SingleStringValueSearchSnapshot extends  AbstractSearchSnapshot {
  private IntList permutationArray;
  private ArrayList<String> unsortedValues;
  private static ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    final String format = DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG);

    protected DecimalFormat initialValue() {
      return new DecimalFormat(format);
    }
  };

  public void init(Object2IntMap<String> map, ReadWriteLock lock) {
    if (unsortedValues != null && unsortedValues.size() == map.size()) {
      // do nothing
      return;
    }
    try {
      lock.readLock().lock();
      if (unsortedValues == null) {
        unsortedValues = new ArrayList<String>(map.size());
      } else {
       int previousSize = unsortedValues.size();       
        for (Entry<String> entry : map.object2IntEntrySet()) {
          if (entry.getIntValue() >= previousSize) {
            unsortedValues.set(entry.getIntValue(), entry.getKey());
          }
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    if (permutationArray == null) {
      permutationArray = new IntArrayList(map.size());
    } else {
      permutationArray.clear();
      permutationArray.size(map.size());
      for (int i = 0; i < map.size(); i++) {
        permutationArray.set(i, i);
      }
    }
    it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.size(), new IntComparator() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }

      @Override
      public int compare(int k1, int k2) {
        String  val1 = unsortedValues.get(permutationArray.getInt(k1));
        String val2 = unsortedValues.get(permutationArray.getInt(k2));
        return val1.compareTo(val2);
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

  
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getStringValue(int)
   */
  @Override
  public String getStringValue(int unsortedDictId) {
    return formatter.get().format(getValue(unsortedDictId));
  }

  private String getValue(int unsortedDictId) {
    if (unsortedDictId < 2) {
      return "";
    }
    return unsortedValues.get(unsortedDictId);
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getObject(int)
   */
  @Override
  public Object getObject(int unsortedDictId) {
    return  getValue(unsortedDictId);
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getInt(int)
   */
  @Override
  public int getIntValue(int unsortedDictId) {
    return Integer.parseInt(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getLong(int)
   */
  @Override
  public long getLongValue(int unsortedDictId) {
    return Long.parseLong(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getDouble(int)
   */
  @Override
  public double getDoubleValue(int unsortedDictId) {
    return Double.parseDouble(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getFloat(int)
   */
  @Override
  public float getFloatValue(int unsortedDictId) {
    return (float)Double.parseDouble(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getShort(int)
   */
  @Override
  public short getShortValue(int unsortedDictId) {
    return (short) Long.parseLong(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.primitives.DictionarySnapshot#getPermutationArray()
   */
  
  @Override
  public void recycle() {
    permutationArray.clear();
    unsortedValues.clear();
    
  }
}
