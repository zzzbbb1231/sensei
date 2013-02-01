package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;

import java.text.DecimalFormat;
import java.util.concurrent.locks.ReadWriteLock;

import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;

public class StringDictionarySnapshot extends AbstractDictionarySnapshot {
  private ObjectList<String> unsortedValues;

  

  public void init(Object2IntMap<String> map, ReadWriteLock lock) {
    if (unsortedValues != null && unsortedValues.size() == map.size()) {
      // do nothing
      return;
    }
    try {
      lock.readLock().lock();
      if (unsortedValues == null) {
        unsortedValues = new ObjectArrayList<String>(map.size());
      } 
      int previousSize = unsortedValues.size(); 
      unsortedValues.size(map.size() );
       for (Entry<String> entry : map.object2IntEntrySet()) {
          if (entry.getIntValue() >= previousSize) {
            unsortedValues.set(entry.getIntValue(), entry.getKey());
          }
        }
      
    } finally {
      lock.readLock().unlock();
    }
    if (permutationArray == null) {
      permutationArray = new IntArrayList(unsortedValues.size());
    } 
      permutationArray.clear();
      permutationArray.size(unsortedValues.size());
      for (int i = 0; i < unsortedValues.size(); i++) {
        permutationArray.set(i, i);
      }
    
    it.unimi.dsi.fastutil.Arrays.quickSort(2, permutationArray.size(), new IntComparator() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return compare(o1.intValue(), o2.intValue());
      }

      @Override
      public int compare(int k1, int k2) {
        String  val1 = unsortedValues.get(permutationArray.getInt(k1));
        String val2 = unsortedValues.get(permutationArray.getInt(k2));
        if (val1 == null  ) {
          if (val2 != null) {
            return -1;
          }
          return 0;
        }
        if (val2 == null) {
          return 1;
        }
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
    invPermutationArray = new IntArrayList(permutationArray.size());
    invPermutationArray.size(permutationArray.size());
      for (int i = 0; i < permutationArray.size(); i++) {
      
        invPermutationArray.set(permutationArray.getInt(i), i);
    }
     
  }

  
  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getStringValue(int)
   */
  @Override
  public String getStringValue(int unsortedDictId) {
    return getValue(unsortedDictId);
  }
  @Override
  public Object getRawValue(int index) {
    Object obj =  getObject(index);
    if (obj == null) {
      return "";
    }
    return obj;
  }
  private String getValue(int unsortedDictId) {
    if (unsortedDictId < 2) {
      return "";
    }
    return unsortedValues.get(unsortedDictId);
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
    return Integer.parseInt(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getLong(int)
   */
  @Override
  public long getLongValue(int unsortedDictId) {
    return Long.parseLong(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getDouble(int)
   */
  @Override
  public double getDoubleValue(int unsortedDictId) {
    return Double.parseDouble(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getFloat(int)
   */
  @Override
  public float getFloatValue(int unsortedDictId) {
    return (float)Double.parseDouble(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getShort(int)
   */
  @Override
  public short getShortValue(int unsortedDictId) {
    return (short) Long.parseLong(getValue(unsortedDictId));
  }

  /* (non-Javadoc)
   * @see com.senseidb.ba.realtime.domain.primitives.DictionarySnapshot#getPermutationArray()
   */
  
  @Override
  public void recycle() {
    permutationArray.clear();
    unsortedValues.clear();
    invPermutationArray.clear();
  }


  @Override
  public TermValueList produceDictionary() {
    TermStringList termStringList = new TermStringList(permutationArray.size() - 2);
    termStringList.add(null);
    for (int i = 2; i < unsortedValues.size(); i++) {
      
      int index = permutationArray.getInt(i);
      if (index <= 1) {
        throw new IllegalStateException();
      }
        String str = unsortedValues.get(index);
        termStringList.add(str);
      

    }
    
    return termStringList;
    
  }
   
 
  @Override
  public ComparableToInt comparableValue(String value) {   
    final String val1 = value;    
    return new ComparableToInt() {      
      @Override
      public int compareTo(int index) {
        String  val2 = unsortedValues.get(permutationArray.getInt(index));
        return -val1.compareTo(val2);
      }
    };
  }
  @Override
  public String format(Object o) {
    if (o == null) {
      return "";
    }
    return o.toString();
  }


  
  
}
