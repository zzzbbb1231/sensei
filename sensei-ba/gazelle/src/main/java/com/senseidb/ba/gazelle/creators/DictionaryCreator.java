package com.senseidb.ba.gazelle.creators;

import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatAVLTreeSet;
import it.unimi.dsi.fastutil.floats.FloatBidirectionalIterator;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongBidirectionalIterator;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.avro.util.Utf8;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;

/**
 * @author dpatel
 */

public class DictionaryCreator {
  public static final Map<ColumnType,String> DEFAULT_FORMAT_STRING_MAP = new HashMap<ColumnType,String>();
  static {
      DEFAULT_FORMAT_STRING_MAP.put(ColumnType.INT, "0000000000");
      DEFAULT_FORMAT_STRING_MAP.put(ColumnType.LONG, "00000000000000000000");
      DEFAULT_FORMAT_STRING_MAP.put(ColumnType.FLOAT, "0000000000.0000");
  }
  private IntAVLTreeSet intAVLTreeSet;
private Int2IntOpenHashMap int2IntMap;
private FloatAVLTreeSet floatAVLTreeSet;
private Float2IntOpenHashMap float2IntMap;
private LongAVLTreeSet longAVLTreeSet;
private Long2IntMap long2IntMap;
private TreeSet<String> stringSet;
private int count;
private Object2IntMap<String> obj2IntMap;

private Object previousValue =  null;
private boolean isSorted = true;
private boolean containsNulls = false;
public DictionaryCreator() {
  intAVLTreeSet = new IntAVLTreeSet();
  longAVLTreeSet = new LongAVLTreeSet();
  stringSet = new TreeSet<String>();
  floatAVLTreeSet = new FloatAVLTreeSet();
}
public boolean contains(Object value, ColumnType columnType) {
  if (columnType == ColumnType.FLOAT) {
    return floatAVLTreeSet.contains(((Number) value).floatValue());
  }
  if (columnType == ColumnType.INT) {
    return intAVLTreeSet.contains(((Number) value).intValue());
  }
  if (columnType == ColumnType.LONG) {
    return longAVLTreeSet.contains(((Number) value).longValue());
  }
  if (columnType == ColumnType.STRING) {
    return stringSet.contains(value.toString());
  }
  throw new UnsupportedOperationException(columnType.toString());
}
public void addValue(Object original, ColumnType type) {
  
  if (original == null) {
    if (isSorted && previousValue != null && containsNulls) {
      isSorted = false;
    }
    containsNulls = true;
    return;
  }
  if (type.isMulti()) {
      isSorted = false;
      
    }
  if (isSorted) {
    if (!original.equals(previousValue) && contains(original, type)) {
      isSorted = false;
      
    } 
    previousValue = original;
    
}
  switch (type) {
    case LONG:
      addLongValue((Long)original);
      break;
    case INT:
      addIntValue((Integer)original);
      break;
    case FLOAT:
      addFloatValue((Float)original);
      break;
    case STRING:
      addStringValue((String)original);
      break;
    case LONG_ARRAY:
        for(Object item : (Object[])original) {
            addLongValue((Long)item);
        }
        break;
      case INT_ARRAY:
          for(Object item : (Object[])original) {
              addIntValue((Integer)item);
          }
        break;
      case FLOAT_ARRAY:
          for(Object item : (Object[])original) {
              addFloatValue((Float)item);
          }
        break;
      case STRING_ARRAY:
          for(Object item : (Object[])original) {
              addStringValue((String)item);
          }
        break;
    default:
      throw new UnsupportedOperationException(original.toString());
  }
}
public void addIntValue(int value) {
  count++;
  intAVLTreeSet.add(value);
}

public void addLongValue(long value) {
  count++;
  longAVLTreeSet.add(value);
}
public void addFloatValue(float value) {
  count++;
  floatAVLTreeSet.add(value);
}
public void addStringValue(String value) {
  count++;
  stringSet.add(value);
}

public int getIntIndex(int value) {
  return int2IntMap.get(value);
}

public int getLongIndex(long value) {
  return long2IntMap.get(value);
}
public int getFloatIndex(float value) {
  return float2IntMap.get(value);
}
public int getStringIndex(String value) {
  return obj2IntMap.get(value);
}
public int getIndex(Object value, ColumnType columnType) {
  if (value == null) {
    count++;
    return 0;
  }
  if (columnType ==  ColumnType.INT) {
    return getIntIndex((Integer) value);
  } else if (columnType ==  ColumnType.LONG) {
    return getLongIndex((Long) value);
  } else if (columnType ==  ColumnType.STRING) {
    return getStringIndex(value.toString());
  } else if (columnType ==  ColumnType.FLOAT) {
    return getFloatIndex((Float) value);
  } else {
    throw new UnsupportedOperationException("" + value);
  }
}
public int[] getIndexes(Object[] values, ColumnType columnType) {
    int[] ret = new int[values.length];
    if (columnType ==  ColumnType.INT_ARRAY) {
      for(int i = 0; i < values.length; i++) {
          ret[i] = getIntIndex((Integer)values[i]);
      }
    } else if (columnType ==  ColumnType.LONG_ARRAY) {
        for(int i = 0; i < values.length; i++) {
            ret[i] = getLongIndex((Long)values[i]);
        }
    } else if (columnType ==  ColumnType.STRING_ARRAY) {
        for(int i = 0; i < values.length; i++) {
            ret[i] = getStringIndex((String)values[i]);
        }
    } else if (columnType ==  ColumnType.FLOAT_ARRAY) {
        for(int i = 0; i < values.length; i++) {
            ret[i] = getFloatIndex((Float)values[i]);
        }
    } else {
      throw new UnsupportedOperationException("" + values);
    }
    return ret;
  }
public int getIndex(Object value) {
  if (value == null) {
    count++;
    return 0;
  }
  if (value instanceof Integer) {
    return getIntIndex((Integer) value);
  } else if (value instanceof Long) {
    return getLongIndex((Long) value);
  } else if (value instanceof String || value instanceof Utf8) {
    return getStringIndex(value.toString());
  } else if (value instanceof Float) {
    return getFloatIndex((Float) value);
  } else if (value instanceof Double) {
    return getFloatIndex(((Double) value).floatValue());
  } else {
    throw new UnsupportedOperationException("" + value);
  }
}

public TermIntList produceIntDictionary() {
  TermIntList termIntList = new TermIntList(intAVLTreeSet.size(),
          DEFAULT_FORMAT_STRING_MAP.get(ColumnType.INT));
  IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
  termIntList.add(null);
  while (iterator.hasNext()) {
    ((IntList) termIntList.getInnerList()).add(iterator.nextInt());
  }
  termIntList.seal();
  int[] elements = termIntList.getElements();
  //in case of negative values
  Arrays.sort(elements, 1, elements.length);
  int2IntMap = new Int2IntOpenHashMap(intAVLTreeSet.size());
  for (int i = 1; i < elements.length; i++) {
    int2IntMap.put(elements[i], i);
  }
  return termIntList;
}
public TermFloatList produceFloatDictionary() {
  TermFloatList termFloatList = new TermFloatList(floatAVLTreeSet.size(),
          DEFAULT_FORMAT_STRING_MAP.get(ColumnType.FLOAT));
  FloatBidirectionalIterator iterator = floatAVLTreeSet.iterator();
  termFloatList.add(null);
  while (iterator.hasNext()) {
    ((FloatList) termFloatList.getInnerList()).add(iterator.nextFloat());
  }
  termFloatList.seal();
  try {
  //in case of negative values
  Field floatField = TermFloatList.class.getDeclaredField("_elements");
  floatField.setAccessible(true);
  float[] elements = (float[]) floatField.get(termFloatList);
  Arrays.sort(elements, 1, elements.length);
  } catch (Exception ex) {
    throw new RuntimeException(ex);
  }
  float2IntMap = new Float2IntOpenHashMap(floatAVLTreeSet.size());
  for (int i = 1; i < termFloatList.size(); i++) {
    float2IntMap.put(termFloatList.getPrimitiveValue(i), i);
  }
  return termFloatList;
}
public Int2IntOpenHashMap getIndexIntMap() {
  return int2IntMap;
}
public TermValueList<?> produceDictionary() {
  if (intAVLTreeSet != null && intAVLTreeSet.size() > 0) {
    return produceIntDictionary();
  }
  if (longAVLTreeSet != null && longAVLTreeSet.size() > 0) {
    return produceLongDictionary();
  }
  if (stringSet != null && stringSet.size() > 0) {
    return produceStringDictionary();
  }
  throw new UnsupportedOperationException();
}
public TermLongList produceLongDictionary() {
  TermLongList termlongList = new TermLongList(longAVLTreeSet.size(),
      DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG));
  LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
  termlongList.add(null);
  while (iterator.hasNext()) {
    ((LongList) termlongList.getInnerList()).add(iterator.nextLong());
  }
  termlongList.seal();
  long[] elements = termlongList.getElements();
  //in case of negative values
  Arrays.sort(elements, 1, elements.length);
  long2IntMap = new Long2IntOpenHashMap(elements.length);
  for (int i = 1; i < elements.length; i++) {
    long2IntMap.put(elements[i], i);
  }
  return termlongList;
}

public TermStringList produceStringDictionary() {
  TermStringList termStringList = new TermStringList(stringSet.size());
  Iterator<String> iterator = stringSet.iterator();
  termStringList.add(null);
  while (iterator.hasNext()) {
    ((List<String>) termStringList.getInnerList()).add(iterator.next());
  }
  termStringList.seal();
  obj2IntMap = new Object2IntOpenHashMap<String>(termStringList.size());
  for (int i = 1; i < termStringList.size(); i++) {
    obj2IntMap.put(termStringList.get(i), i);
  }
  return termStringList;
}
  
public boolean isSorted() {
      return isSorted;
  }

  public int getCount() {
  return count;
}
}
