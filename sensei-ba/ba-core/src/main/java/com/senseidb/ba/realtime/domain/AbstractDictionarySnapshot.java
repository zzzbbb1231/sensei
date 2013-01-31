package com.senseidb.ba.realtime.domain;

import java.util.List;

import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.SortUtil;
import com.senseidb.ba.gazelle.utils.SortUtil.ComparableToInt;

import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.ints.IntList;

public abstract class AbstractDictionarySnapshot extends TermNumberList implements DictionarySnapshot {
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
  
  public abstract ComparableToInt comparableValue(String value);
  
  public boolean add(Object e) {
    throw new UnsupportedOperationException();
  }


  public Object set(int index, Object element) {
    throw new UnsupportedOperationException();
  }


  public void add(int index, Object element) {
    throw new UnsupportedOperationException();
    
  }


  protected List buildPrimitiveList(int capacity) {
    return null;
  }


  public String format(Object o) {
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


  public boolean containsWithType(Object val) {
    throw new UnsupportedOperationException();
  }

  public int indexOfWithType(Object o) {
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
 public IntList getInvPermutationArray() {
   return invPermutationArray;
 }
  
  @Override
public Class getType() {
  return Object.class;
}
 @Override
 protected Object parseString(String o) {
   throw new UnsupportedOperationException();
 }
}
