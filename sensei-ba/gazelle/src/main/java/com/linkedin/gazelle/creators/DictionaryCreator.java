package com.linkedin.gazelle.creators;

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

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.GazelleColumnType;
import com.linkedin.gazelle.utils.GazelleUtils;

/**
 * @author dpatel
 */

public class DictionaryCreator {

  private static Logger logger = Logger.getLogger(DictionaryCreator.class);
  /*
   * Tree Sets
   */

  private IntAVLTreeSet _intAVLTreeSet;
  private FloatAVLTreeSet _floatAVLTreeSet;
  private LongAVLTreeSet _longAVLTreeSet;
  private TreeSet<String> _stringSet;
  private Int2IntOpenHashMap _int2IntMap;
  private Float2IntOpenHashMap _float2IntMap;
  private Long2IntMap _long2IntMap;
  private Object2IntMap<String> _obj2IntMap;
  private TermIntList _termIntList;
  private TermLongList _termLongList;
  private TermFloatList _termFloatList;
  private TermStringList _termStringList;

  int _counter;

  public DictionaryCreator() {
    _counter = 1;
    /*
     * Tree Sets Intialization
     */
    _intAVLTreeSet = new IntAVLTreeSet();
    _longAVLTreeSet = new LongAVLTreeSet();
    _stringSet = new TreeSet<String>();
    _floatAVLTreeSet = new FloatAVLTreeSet();
  }


  public void addValue(Object original, GazelleColumnType type) {
    switch (type) {
      case LONG:
        addLongValue(original);
        break;
      case INT:
        addIntValue(original);
        break;
      case FLOAT:
        addFloatValue(original);
        break;
      case STRING:
        addStringValue(original);
        break;
      default:
        throw new UnsupportedOperationException(original.toString());
    }
  }

  public int getCount() {
    return _counter;
  }

  private void addIntValue(Object o) {
    if (_intAVLTreeSet.add(((Integer) o).intValue())) {
      _counter++;
    }
    
  }

  private void addStringValue(Object o) {
    if (_stringSet.add((String) o)) {
      _counter++;
    }
  }

  private void addLongValue(Object o) {
    if (_longAVLTreeSet.add(((Long) o).longValue())) {
      _counter++;
    }
  }

  private void addFloatValue(Object o) {
    if (_floatAVLTreeSet.add(((Float) o).floatValue())) {
      _counter++;
    }
  }

  private int getIntIndex(int value) {
    return _int2IntMap.get(value);
  }

  private int getLongIndex(long value) {
    return _long2IntMap.get(value);
  }

  private int getFloatIndex(float value) {
    return _float2IntMap.get(value);
  }

  private int getStringIndex(String value) {
    return _obj2IntMap.get(value);
  }

  public int getValue(Object value, GazelleColumnType type) {
    if (value == null) {
      return 0;
    }
    switch (type) {
      case LONG:
        return getLongIndex(((Long) value).longValue());
      case INT:
        return getIntIndex(((Integer) value).intValue());
      case FLOAT:
        return getFloatIndex(((Float) value).floatValue());
      case STRING:
        return getStringIndex((String) value);
      default:
        throw new IllegalStateException();
    }
  }

  public TermValueList getTermValueList(GazelleColumnType type) {
    switch (type) {
      case LONG:
        return getTermLongList();
      case INT:
        return getTermIntList();
      case FLOAT:
        return getTermFloatList();
      case STRING:
        return getTermStringList();
      default:
        throw new IllegalStateException();
    }
  }

  private TermValueList getTermIntList() {
    _termIntList = new TermIntList(_intAVLTreeSet.size(), GazelleUtils.INT_STRING_REPSENTATION);
    IntBidirectionalIterator iterator = _intAVLTreeSet.iterator();
    _termIntList.add(null);
    while (iterator.hasNext()) {
      ((IntList) _termIntList.getInnerList()).add(iterator.nextInt());
    }
    _termIntList.seal();
    int[] elements = _termIntList.getElements();
    _int2IntMap = new Int2IntOpenHashMap(_intAVLTreeSet.size());
    for (int i = 1; i < elements.length; i++) {
      _int2IntMap.put(elements[i], i);
    }
    return _termIntList;
  }

  private TermValueList getTermLongList() {
    _termLongList = new TermLongList(_longAVLTreeSet.size(), GazelleUtils.LONG_STRING_REPSENTATION);
    LongBidirectionalIterator iterator = _longAVLTreeSet.iterator();
    _termLongList.add(null);
    while (iterator.hasNext()) {
      ((LongList) _termLongList.getInnerList()).add(iterator.nextLong());
    }
    _termLongList.seal();
    long[] elements = _termLongList.getElements();
    _long2IntMap = new Long2IntOpenHashMap(elements.length);
    for (int i = 1; i < elements.length; i++) {
      _long2IntMap.put(elements[i], i);
    }
    return _termLongList;
  }

  private TermValueList getTermFloatList() {
    _termFloatList = new TermFloatList(_floatAVLTreeSet.size(), GazelleUtils.FLOAT_STRING_REPSENTATION);
    FloatBidirectionalIterator iterator = _floatAVLTreeSet.iterator();
    _termFloatList.add(null);
    while (iterator.hasNext()) {
      ((FloatList) _termFloatList.getInnerList()).add(iterator.nextFloat());
    }
    _termFloatList.seal();
    _float2IntMap = new Float2IntOpenHashMap(_floatAVLTreeSet.size());
    for (int i = 1; i < _termFloatList.size(); i++) {
      _float2IntMap.put(_termFloatList.getPrimitiveValue(i), i);
    }
    return _termFloatList;
  }

  @SuppressWarnings("unchecked")
  private TermValueList getTermStringList() {
    _termStringList = new TermStringList(_stringSet.size());
    Iterator<String> iterator = _stringSet.iterator();
    _termStringList.add(null);
    while (iterator.hasNext()) {
      ((List<String>) _termStringList.getInnerList()).add(iterator.next());
    }
    _termStringList.seal();
    _obj2IntMap = new Object2IntOpenHashMap<String>(_termStringList.size());
    for (int i = 1; i < _termStringList.size(); i++) {
      _obj2IntMap.put(_termStringList.get(i), i);
    }
    return _termStringList;
  }
}
