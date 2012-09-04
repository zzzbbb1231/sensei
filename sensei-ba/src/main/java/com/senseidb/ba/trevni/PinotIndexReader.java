package com.senseidb.ba.trevni;

import java.util.Map;

import com.senseidb.ba.trevni.impl.TermValueList;

public interface PinotIndexReader {
  Map<String, Class<?>> getColumnTypes();
  /**
  * get the sorted array of unique column values
  */
 TermValueList getDictionary(String column);
  /**
  * Only dimension columns need inverted index. We might use kamikaze's implementation of the p4delta compression for the inverted index http://sna-projects.com/kamikaze/quickstart.php
  * If the column's cardinality is < 100 we might use compressed bitset index instead of p4delta 
  * We maintain the inverted index per each column value. 
  */
  DocIdSet[] getInvertedIndex(String column);
  ForwardIndex  getForwardIndex(String column);
  /**
  * number of docs in the index
  */
 int getLength();
}
