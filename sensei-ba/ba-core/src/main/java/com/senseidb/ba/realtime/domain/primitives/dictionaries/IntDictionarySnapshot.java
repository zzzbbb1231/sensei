package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.SortUtil;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

public class IntDictionarySnapshot extends TermIntList implements DictionarySnapshot {

  private IntList unsortedValues;
  private DictionaryResurrectingMarker dictionaryResurrectingMarker;
  public IntDictionarySnapshot(ReusableIndexObjectsPool indexObjectsPool, String columnName) {
    dictionaryResurrectingMarker = new DictionaryResurrectingMarker(columnName, indexObjectsPool, this);
  }
  private static ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    final String format = DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.INT);

    protected DecimalFormat initialValue() {
      return new DecimalFormat(format);
    }
  };
 
  public void init(Int2IntMap map, ReadWriteLock lock) {
    if (unsortedValues != null && unsortedValues.size() == map.size()) {
      // do nothing
      return;
    }
    try {
      lock.readLock().lock();
      if (unsortedValues == null) {
        unsortedValues = new IntArrayList(map.size());
      }
       int previousSize = unsortedValues.size();
        unsortedValues.size(map.size());
        for (it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry : map.int2IntEntrySet()) {
            unsortedValues.set(entry.getIntValue(), entry.getIntKey());
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
   
      SortUtil.quickSort(0, permutationArray.size(), new SortUtil.IntComparator() {
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
    }, new SortUtil.Swapper() {
      @Override
      public void swap(int a, int b) {
        int tmp = permutationArray.getInt(b);
        permutationArray.set(b, permutationArray.get(a));
        permutationArray.set(a, tmp);
      }
    });
    invDictPermArray = new IntArrayList(permutationArray.size());
    invDictPermArray.size(permutationArray.size());
      for (int i = 0; i < permutationArray.size(); i++) {
      
        invDictPermArray.set(permutationArray.getInt(i), i);
    }
  }

 
  @Override
  public String getStringValue(int unsortedDictId) {
    return formatter.get().format(getValue(unsortedDictId));
  }

  private int getValue(int unsortedDictId) {
    if (unsortedDictId < 2) {
      return 0;
    }
    return unsortedValues.getInt(unsortedDictId);
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getObject(int)
   */
  @Override
  public Object getObject(int unsortedDictId) {
    return  getValue(unsortedDictId);
  }
  @Override
  public Integer getRawValue(int index) {
    Integer obj =  getIntValue(index);
    if (obj == null) {
      return Integer.MIN_VALUE;
    }
    return obj;
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
    invDictPermArray.clear();
  }


  @Override
  public TermValueList produceDictionary() {
    try {
    final int[] longArr = new int[unsortedValues.size() - 1];
    for (int i = 0; i < unsortedValues.size(); i++) {
      int index = permutationArray.get(i);
      if (index == 0) {
        continue;
      }
      if (index == 1) {
        longArr[0] = TermIntList.VALUE_MISSING;
        continue;
      }
      longArr[i - 1] = unsortedValues.getInt(index);
    }
    TermIntList termIntList =
        new TermIntList(unsortedValues.size(), DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.INT)) {
          public int size() {
            return longArr.length;
          }
          @Override
          public Integer getRawValue(int index) {
            return super.getPrimitiveValue(index);
          }
        };
    Field intField;
   
      intField = TermIntList.class.getDeclaredField("_elements");
    
    intField.setAccessible(true);
    intField.set(termIntList, longArr);
    return termIntList;
    } catch (Exception e) {
     throw new RuntimeException(e);
    }
  }


  public ComparableToInt comparableValue(String value) {   
    final int val1 = Integer.parseInt(value);    
    return new ComparableToInt() {      
      @Override
      public int compareTo(int index) {
        int val2 = unsortedValues.getInt(permutationArray.getInt(index));
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
private IntArrayList invDictPermArray;

public int size() {
  return permutationArray.size();
}

public IntList getDictPermutationArray() {
  return permutationArray;
}

public int sortedIndexOf(String value) {
  if (value == null || value.length() == 0) {
    return 1;
  }
  return SortUtil.binarySearch(2, permutationArray.size(), comparableValue(value));
}



public boolean add(Long e) {
  throw new UnsupportedOperationException();
}


public Object set(int index, Long element) {
  throw new UnsupportedOperationException();
}


public void add(int index, Long element) {
  throw new UnsupportedOperationException();
  
}


protected List buildPrimitiveList(int capacity) {
  return null;
}


public String format(Long o) {
  throw new UnsupportedOperationException();
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


public boolean containsWithType(Long val) {
  throw new UnsupportedOperationException();
}

public int indexOfWithType(Long o) {
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
@Override
public Class getType() {
return Integer.class;
}
@Override
public int getPrimitiveValue(int index) {
 
  return getIntValue(index);
}
@Override
protected Object parseString(String o) {
 throw new UnsupportedOperationException();
}


@Override
public IntList getInvPermutationArray() {
  return invDictPermArray;
}


@Override
public DictionaryResurrectingMarker getResurrectingMarker() {
  return dictionaryResurrectingMarker;
}

  
}
