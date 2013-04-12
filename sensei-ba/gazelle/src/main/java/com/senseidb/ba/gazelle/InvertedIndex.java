package com.senseidb.ba.gazelle;

/**
 * A general interface to house and produce Inverted Index Sets (DocIdSets)
 */

import org.apache.lucene.search.DocIdSet;

import com.senseidb.ba.gazelle.impl.InvertedIndexStatistics;

public interface InvertedIndex{

	/**
	 * Returns the DocIdSet of the corresponding dictValue input.
	 * @param dictValue
	 */
	public DocIdSet getSet(int dictValue);
	
	/**
	 * The number of values for the column this inverted index represents
	 */
	public int length();
	
	/**
	 * Check if the given dictionary value has an inverted index set associated with it.
	 * @param dictionaryIndex
	 */
	public boolean invertedIndexPresent(int dictionaryIndex);
	
	/**
	 * @return The index stats for this column.
	 */
	public InvertedIndexStatistics getIndexStatistics();
	
}