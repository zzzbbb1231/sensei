package com.senseidb.ba.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.search.DocIdSetIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.facet.FacetUtils;
import com.senseidb.ba.facet.MultiFacetUtils;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

public class RangeQueryFacetHandlerTest {

  static GazelleIndexSegmentImpl segmentImpl;
  static HashMap<String, SingleValueForwardIndex> singleValyeForwardIndexMap;
  static HashMap<String, SortedForwardIndex> sortedForwardIndexMap;
  static HashMap<String, MultiValueForwardIndex> multiValueForwardIndexMap;

  @AfterClass
  public static void tearDown() throws Exception {

  }

  @BeforeClass
  public static void setUp() throws Exception {
    segmentImpl = TestUtil.createIndexSegment();
    singleValyeForwardIndexMap = new HashMap<String, SingleValueForwardIndex>();
    sortedForwardIndexMap = new HashMap<String, SortedForwardIndex>();
    multiValueForwardIndexMap = new HashMap<String, MultiValueForwardIndex>();
    for (String column : segmentImpl.getForwardIndexes().keySet()) {
      if (segmentImpl.getForwardIndex(column) instanceof SingleValueForwardIndex) {
        singleValyeForwardIndexMap.put(column, (SingleValueForwardIndex) segmentImpl.getForwardIndex(column));
      } else if (segmentImpl.getForwardIndex(column) instanceof SortedForwardIndex) {
        sortedForwardIndexMap.put(column, (SortedForwardIndex) segmentImpl.getForwardIndex(column));
      } else if (segmentImpl.getForwardIndex(column) instanceof MultiValueForwardIndex) {
        multiValueForwardIndexMap.put(column, (MultiValueForwardIndex) segmentImpl.getForwardIndex(column));
      }
    }
  }

  @Test
  public void testRangeQueryOnSingleValueForwardIndex() throws Exception {
    String colName = "dim_memberIndustry";
    SingleValueForwardIndex forwardIndex = singleValyeForwardIndexMap.get(colName);
    TermValueList<?> termList = segmentImpl.getDictionary(colName);
    String someStartVal = termList.get(termList.size() - 10);
    String someEndVal = termList.get(termList.size() - 5);
    int startIndex = segmentImpl.getDictionary(colName).indexOf(someStartVal);
    int endIndex = segmentImpl.getDictionary(colName).indexOf(someEndVal);
    
    FacetUtils.RangeForwardDocIdSet docIdSet = new FacetUtils.RangeForwardDocIdSet((GazelleForwardIndexImpl)forwardIndex, startIndex, endIndex);
    DocIdSetIterator iterator = docIdSet.iterator();
    
    int countFromDocIdSetIterator = 0;
    
    while(true) {
      int doc = iterator.nextDoc();
      if ( doc == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      } else {
        countFromDocIdSetIterator++;
      }
    }
    
    int countFromForwardIndexScan = 0;
    
    for (int i = 0 ; i < forwardIndex.getLength(); i++) {
      if (forwardIndex.getValueIndex(i) >= startIndex && forwardIndex.getValueIndex(i) <= endIndex) {
        countFromForwardIndexScan++;
      }
    }
    
    assertEquals(countFromDocIdSetIterator, countFromForwardIndexScan);
  }
}
