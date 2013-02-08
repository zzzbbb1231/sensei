package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.floats.Float2IntMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.SortUtil;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

public class FloatDictionarySnapshot extends TermFloatList implements DictionarySnapshot {

  private FloatList unsortedValues;

  private DictionaryResurrectingMarker dictionaryResurrectingMarker;
  
  private static ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    final String format = DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.FLOAT);

    protected DecimalFormat initialValue() {
      return new DecimalFormat(format);
    }
  };
  public FloatDictionarySnapshot(ReusableIndexObjectsPool indexObjectsPool, String columnName) {
    dictionaryResurrectingMarker = new DictionaryResurrectingMarker(columnName, indexObjectsPool, this);
  }
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
            unsortedValues.set(entry.getIntValue(), entry.getFloatKey());
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
    invPermutationArray = new IntArrayList(permutationArray.size());
    invPermutationArray.size(permutationArray.size());
      for (int i = 0; i < permutationArray.size(); i++) {
      
        invPermutationArray.set(permutationArray.getInt(i), i);
    }
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
    invPermutationArray.clear();
  }
  @Override
  public Float getRawValue(int index) {
   Float obj =  getValue(index);
   if (obj == null) {
     return Float.MIN_VALUE;
   }
   return obj;
  }

  @Override
  public TermValueList produceDictionary() {
    try {
    final float[] floatArr = new float[unsortedValues.size() - 1];
    for (int i = 0; i < unsortedValues.size(); i++) {
      int index = permutationArray.get(i);
      if (i == 0) {
        continue;
      }
      if (i == 1) {
        floatArr[0] = Float.NEGATIVE_INFINITY;
        continue;
      }
      floatArr[i - 1] = unsortedValues.getFloat(index);
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
          @Override
          public int indexOf(Object o)
          {
            float val;
            if (o instanceof String)
              val = Float.parseFloat((String) o);
            else
              val = (Float)o;
            
            return Arrays.binarySearch(floatArr, val);
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
    if (o == null) {
      return "";
    }
    return formatter.get().format(o);
  }
  protected IntList permutationArray;
  protected IntList invPermutationArray;
  @Override
  public int size() {
    return permutationArray.size();
  }
  @Override
  public IntList getDictPermutationArray() {
    return permutationArray;
  }
  
  public int sortedIndexOf(String value) {
    if (value == null || value.length() == 0) {
      return 1;
    }
    return SortUtil.binarySearch(2, permutationArray.size(), comparableValue(value));
  }
  
  
  
  public boolean add(Float e) {
    throw new UnsupportedOperationException();
  }


  public Object set(int index, Float element) {
    throw new UnsupportedOperationException();
  }


  public void add(int index, Float element) {
    throw new UnsupportedOperationException();
    
  }


  protected List buildPrimitiveList(int capacity) {
    return null;
  }


  


  public void seal() {
    throw new UnsupportedOperationException();
    
  }


  public boolean add(String o) {
    throw new UnsupportedOperationException();
  }


  public boolean addRaw(Object o) {
    throw new UnsupportedOperationException();
  }


  public boolean containsWithType(Float val) {
    throw new UnsupportedOperationException();
  }

  public int indexOfWithType(Float o) {
    return 0;
  }
  @Override
  public int indexOf(Object o) {
    if (o == null) {
      return 1;
    }
    int indexOf = sortedIndexOf(o.toString());
    if (indexOf < 0) {
      return indexOf;
    }
    return getDictPermutationArray().getInt(indexOf);
  }
  public float getPrimitiveValue(int index)
  {
    return getFloatValue(index);
  }
  @Override
  public String get(int index) {
   return format(getObject(index));
  }
  @Override
  public Comparable getComparableValue(int index) {
    
    Object object = getObject(index);
    if (object == null) {
      return new Comparable<Comparable>() {
        @Override
        public int compareTo(Comparable o) {
         if (o.getClass() == this.getClass())
          return 0;
         return -1;
        }
      };
    }
    return (Comparable)object;
  }
 public IntList getInvPermutationArray() {
   return invPermutationArray;
 }
  
  @Override
public Class getType() {
  return Float.class;
}
 @Override
 protected Object parseString(String o) {
   throw new UnsupportedOperationException();
 }
@Override
public DictionaryResurrectingMarker getResurrectingMarker() {
  return dictionaryResurrectingMarker;
}




}
