package com.senseidb.ba;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.avro.util.Utf8;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.indexing.DefaultSenseiInterpreter;

import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.FloatAVLTreeSet;
import it.unimi.dsi.fastutil.floats.FloatBidirectionalIterator;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
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

public class DictionaryCreator {
	private IntAVLTreeSet intAVLTreeSet;
	private Int2IntOpenHashMap int2IntMap;
	private FloatAVLTreeSet floatAVLTreeSet;
	private Float2IntOpenHashMap float2IntMap;
	private LongAVLTreeSet longAVLTreeSet;
	private Long2IntMap long2IntMap;
	private TreeSet<String> stringSet;
	private int count;
	private Object2IntMap<String> obj2IntMap;

	public DictionaryCreator() {
		intAVLTreeSet = new IntAVLTreeSet();
		longAVLTreeSet = new LongAVLTreeSet();
		stringSet = new TreeSet<String>();
		floatAVLTreeSet = new FloatAVLTreeSet();
	}

	public void add(Object value) {
		if (value instanceof Integer) {
			addIntValue((Integer) value);
		} else if (value instanceof Long) {
			addLongValue((Long) value);
		} else if (value instanceof String) {
			addStringValue((String) value);
		} else {
			throw new UnsupportedOperationException("" + value);
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
				DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP
						.get(int.class));
		IntBidirectionalIterator iterator = intAVLTreeSet.iterator();
		termIntList.add(null);
		while (iterator.hasNext()) {
			((IntList) termIntList.getInnerList()).add(iterator.nextInt());
		}
		termIntList.seal();
		int[] elements = termIntList.getElements();
		int2IntMap = new Int2IntOpenHashMap(intAVLTreeSet.size());
		for (int i = 1; i < elements.length; i++) {
			int2IntMap.put(elements[i], i);
		}
		return termIntList;
	}
	public TermFloatList produceFloatDictionary() {
		TermFloatList termFloatList = new TermFloatList(floatAVLTreeSet.size(),
				DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP
						.get(float.class));
		FloatBidirectionalIterator iterator = floatAVLTreeSet.iterator();
		termFloatList.add(null);
		while (iterator.hasNext()) {
			((FloatList) termFloatList.getInnerList()).add(iterator.nextFloat());
		}
		termFloatList.seal();
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
				DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP
						.get(long.class));
		LongBidirectionalIterator iterator = longAVLTreeSet.iterator();
		termlongList.add(null);
		while (iterator.hasNext()) {
			((LongList) termlongList.getInnerList()).add(iterator.nextLong());
		}
		termlongList.seal();
		long[] elements = termlongList.getElements();
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

	public int getCount() {
		return count;
	}
	
}
