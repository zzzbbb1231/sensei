package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

/**
 * A specialized inverted index class for gazelle. This is only used if the number of values in the dictionary are too high to be able to use the
 * normal GazelleInvertedIndexImpl without taking too long to initialize them or taking up too much space. In this implementation, compression is
 * a minimum, but initialization and iteration time remains similar.
 */

public class GazelleInvertedIndexHighCardinalityImpl {

	private int[] offsets = null;	
	private int[] data = null;

	private static final Counter invertedDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedDocCount");
	private static final Counter invertedCompressedSize = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedCompressedSize");
	private static final Counter invertedTotalDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedTotalDocCount");

	/** 
	 * This function prepares the data to be read. This MUST be called after the initializer before using the iterator.
	 */
	public void prepData(){

		offsets[0] = 0;

		for(int i = offsets.length - 1; i > 0; i--){
			offsets[i] = offsets[i - 1];
		}

		invertedDocCount.inc(data.length);
		invertedTotalDocCount.inc(data.length);
		invertedCompressedSize.inc(data.length * 4);

	}

	/**
	 * The initializer loads all the DocIDs into the class and initializes the offset array.
	 * @param fIndex -> Used to read through the forward index
	 * @param valCount -> Number of dictionary values for this column
	 */
	public GazelleInvertedIndexHighCardinalityImpl(ForwardIndex fIndex, int valCount){

		offsets = new int[valCount + 1];
		int size = fIndex.getLength();

		if(fIndex instanceof SingleValueForwardIndex){

			SingleValueForwardIndex index = (SingleValueForwardIndex) fIndex;
			SingleValueRandomReader ireader = index.getReader();
			for(int i = 0; i < size; i++){
				offsets[ireader.getValueIndex(i)]++;
			}

			for(int i = 1; i < offsets.length; i++){
				offsets[i] += offsets[i - 1];
			}

			size = fIndex.getLength();
			data = new int[size];

			for(int i = 1; i < size; i++){
				if(ireader.getValueIndex(i) == 0){
					continue;
				}
				data[offsets[ireader.getValueIndex(i) - 1]++] = i;
			}

		}
		else{
			MultiValueForwardIndexImpl1 index = (MultiValueForwardIndexImpl1) fIndex;
			int[] buffer = new int[index.getMaxNumValuesPerDoc()];

			for(double i = 0; i < size; i++){
				int count = index.randomRead(buffer, (int) i);
				for(int j = 0; j < count; j++){
					offsets[buffer[j]]++;
				}
			}
			
			for(int i = 1; i < offsets.length; i++){
				offsets[i] += offsets[i - 1];
			}
			
			size = offsets[offsets.length];
			data = new int[size];
			
			for(double i = 0; i < size; i++){
				int count = index.randomRead(buffer, (int) i);
				for(int j = 0; j < count; j++){
					if(buffer[j] == 0){
						continue;
					}
					data[offsets[buffer[j] - 1]++] = (int) i;
				}
			}
		}

	}

	/**
	 * Returns the DocIdSet for the specified dictValue
	 * @param dictValue
	 */
	public GazelleInvertedHighCardinalitySet getSet(int dictValue){
		return new GazelleInvertedHighCardinalitySet(dictValue); 
	}

	public static long getTotalCount() {
		return invertedTotalDocCount.count();
	}

	public static long getTotalTrueCount() {
		return invertedDocCount.count();
	}

	public static long getTotalCompSize() {
		return invertedCompressedSize.count();
	}

	class GazelleInvertedHighCardinalitySet extends DocIdSet {

		private int dictValue = 0;

		GazelleInvertedHighCardinalitySet(int dictValue){
			this.dictValue = dictValue - 1;
		}

		@Override
		public DocIdSetIterator iterator() throws IOException {
			return new GazelleInvertedIndex(dictValue);
		}

		/**
		 * This class is used to iterate through the DocID set (Inverted index). The calls work the same way as Lucene's inverted index iterators.
		 * @author jjung
		 *
		 */
		class GazelleInvertedIndex extends DocIdSetIterator {

			private int readIndex = 0;
			private int end = 0;
			private int lastDoc = 0;

			GazelleInvertedIndex() throws IOException{
				super();			
			}

			public GazelleInvertedIndex(int value) {
				super();
				readIndex = offsets[value];
				end = offsets[value + 1];
			}

			@Override
			public int docID() {
				return lastDoc;
			}

			@Override
			public int nextDoc() throws IOException {

				if(readIndex < end){
					lastDoc = data[readIndex++];
					return lastDoc;
				}

				else lastDoc = DocIdSetIterator.NO_MORE_DOCS;
				return lastDoc;


			}

			@Override
			public int advance(int target) throws IOException {

				while(readIndex < end){
					if(data[readIndex] >= target){
						lastDoc = data[readIndex++];
						return lastDoc;
					}
					readIndex++;
				}

				lastDoc = DocIdSetIterator.NO_MORE_DOCS;
				return lastDoc;

			}
		}
	}

}
