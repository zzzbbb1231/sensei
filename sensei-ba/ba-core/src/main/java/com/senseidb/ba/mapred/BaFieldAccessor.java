package com.senseidb.ba.mapred;

import java.util.HashMap;
import java.util.Map;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.facet.ZeusDataCache;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;
import com.senseidb.search.req.mapred.FieldAccessor;

public class BaFieldAccessor implements FieldAccessor {
  public static final class DocIdMapper implements DocIDMapper {
    public static final DocIdMapper instance = new DocIdMapper();
    @Override
    public int getDocID(long uid) {
      return (int)(uid & 0xFFFFFFFF);
    }
    @Override
    public int quickGetDocID(long uid) {
      return (int)(uid & 0xFFFFFFFF);
    }
    @Override
    public int getReaderIndex(long uid) {
      return (int)(uid & 0xFFFFFFFF);
    }
    @Override
    public ZoieIndexReader[] getSubReaders() {
      return null;
    }
    @Override
    public int[] getStarts() {
      return null;
    }
    @Override
    public Object getDocIDArray(long[] uids) {
      return null;
    }
    @Override
    public Object getDocIDArray(int[] uids) {
      return null;
    }
  }

  private final IndexSegment indexSegment;
  private Map<String, SingleValueRandomReader> randomReaders = new HashMap<String, SingleValueRandomReader>();
  private Map<String, MultiFacetIterator> multiIndexes = new HashMap<String, MultiFacetIterator>();
  private int[] buffer;
  private final SegmentToZoieReaderAdapter segmentToZoieReaderAdapter;
  
  @SuppressWarnings("rawtypes")
  public BaFieldAccessor(SegmentToZoieReaderAdapter segmentToZoieReaderAdapter, String segmentName) {
    this.segmentToZoieReaderAdapter = segmentToZoieReaderAdapter;
    this.indexSegment = segmentToZoieReaderAdapter.getOfflineSegment();  
    int maxBufferSize = 0;
    for (String column : indexSegment.getColumnTypes().keySet()) {
      ForwardIndex forwardIndex = indexSegment.getForwardIndex(column);
      if (forwardIndex instanceof SingleValueForwardIndex) {
        randomReaders.put(column, ((SingleValueForwardIndex) forwardIndex).getReader());
      } else if (forwardIndex instanceof MultiValueForwardIndexImpl1) {
        MultiValueForwardIndexImpl1 forwardIndex2 = (MultiValueForwardIndexImpl1) forwardIndex;
        multiIndexes.put(column, forwardIndex2.getIterator());
        if (maxBufferSize < forwardIndex2.getMaxNumValuesPerDoc()) {
          maxBufferSize = forwardIndex2.getMaxNumValuesPerDoc();
        }
      }
      buffer = new int[maxBufferSize];
    }
  }
  @Override
  public ZeusDataCache getValueCache(String name) {
    return (ZeusDataCache) ((BoboIndexReader)segmentToZoieReaderAdapter.getInnerReader()).getFacetData(name);
  }

  @Override
  public Object get(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return null;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      return indexSegment.getDictionary(fieldName).getRawValue(valueId);
    } else {      
      MultiFacetIterator multi = multiIndexes.get(fieldName);
      if (multi == null) {
        return null;
      }
      multi.advance(docId);
      int count = multi.readValues(buffer);
      Object[] ret = new Object[count];
      for (int i = 0; i < count; i++) {
        ret[i] = dictionary.getRawValue(buffer[i]);
      }
      return ret;
    }
  }

  @Override
  public String getString(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return null;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);     
      return indexSegment.getDictionary(fieldName).get(valueId);
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
  }

  @Override
  public long getLong(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return Long.MIN_VALUE;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      if (dictionary instanceof TermLongList) {
        return ((TermLongList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermIntList) {
        return ((TermIntList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermFloatList) {
        return (long)((TermFloatList)dictionary).getPrimitiveValue(valueId);
      }
      return Long.parseLong(dictionary.get(valueId));      
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
  }

  @Override
  public double getDouble(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return Double.MIN_VALUE;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      if (dictionary instanceof TermLongList) {
        return ((TermLongList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermIntList) {
        return ((TermIntList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermFloatList) {
        return ((TermFloatList)dictionary).getPrimitiveValue(valueId);
      }
      return Double.parseDouble(dictionary.get(valueId));      
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
    
  }

  @Override
  public short getShort(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return Short.MIN_VALUE;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      if (dictionary instanceof TermLongList) {
        return (short)((TermLongList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermIntList) {
        return (short)((TermIntList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermFloatList) {
        return (short)((TermFloatList)dictionary).getPrimitiveValue(valueId);
      }
      return (short)Short.parseShort(dictionary.get(valueId));      
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
  }

  @Override
  public int getInteger(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return Integer.MIN_VALUE;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      if (dictionary instanceof TermIntList) {
        return ((TermIntList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermLongList) {
        return (int)((TermLongList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermFloatList) {
        return (int)((TermFloatList)dictionary).getPrimitiveValue(valueId);
      }
      return Integer.parseInt(dictionary.get(valueId));      
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
  }

  @Override
  public float getFloat(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return Float.MIN_VALUE;
    }
    SingleValueRandomReader singleValueRandomReader = randomReaders.get(fieldName);
    if (singleValueRandomReader != null) {
      int valueId = singleValueRandomReader.getValueIndex(docId);
      if (dictionary instanceof TermLongList) {
        return ((TermLongList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermIntList) {
        return ((TermIntList)dictionary).getPrimitiveValue(valueId);
      }
      if (dictionary instanceof TermFloatList) {
        return ((TermFloatList)dictionary).getPrimitiveValue(valueId);
      }
      return Float.parseFloat(dictionary.get(valueId));      
    } else {      
     throw new IllegalStateException("The column is the multi value - " + fieldName);
    }
  }

  @Override
  public Object[] getArray(String fieldName, int docId) {
    TermValueList<?> dictionary = indexSegment.getDictionary(fieldName);
    if (dictionary == null) {
      return null;
    }
    MultiFacetIterator multi = multiIndexes.get(fieldName);
    if (multi == null) {
      return null;
    }
    multi.advance(docId);
    int count = multi.readValues(buffer);
    Object[] ret = new Object[count];
    for (int i = 0; i < count; i++) {
      ret[i] = dictionary.getRawValue(buffer[i]);
    }
    return ret;
  }

  @Override
  public Object getByUID(String fieldName, long uid) {
    
    return get(fieldName, (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public String getStringByUID(String fieldName, long uid) {
   
    return getString(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public long getLongByUID(String fieldName, long uid) {
    return getLong(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public double getDoubleByUID(String fieldName, long uid) {
    return  getDouble(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public short getShortByUID(String fieldName, long uid) {
    
    return getShort(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public int getIntegerByUID(String fieldName, long uid) {
    return getInteger(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public float getFloatByUID(String fieldName, long uid) {
    return getFloat(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public Object[] getArrayByUID(String fieldName, long uid) {
    return getArray(fieldName , (int)(uid & 0xFFFFFFFF));
  }

  @Override
  public TermValueList getTermValueList(String fieldName) {
    return indexSegment.getDictionary(fieldName);
  }

  @Override
  public FacetHandler getFacetHandler(String facetName) {
    return ((BoboIndexReader)segmentToZoieReaderAdapter.getInnerReader()).getFacetHandler(facetName);
  }

  @Override
  public BoboIndexReader getBoboIndexReader() {
    return (BoboIndexReader)segmentToZoieReaderAdapter.getInnerReader();
  }

  @Override
  public DocIDMapper getMapper() {
    return  DocIdMapper.instance;
  }
  public SingleValueRandomReader getReader(String column) {
    return randomReaders.get(column);
  }
}
