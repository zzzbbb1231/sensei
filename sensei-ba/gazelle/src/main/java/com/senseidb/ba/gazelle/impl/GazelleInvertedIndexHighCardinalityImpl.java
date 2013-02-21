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
 * Implementation of an InvertedIndex for SenseiBA. We don't store all docIDs in
 * our inverted index; but we opt to keep large 'jumps' and rely on iterating
 * through the forward index for smaller gaps. The size of this jump is calculated
 * so that compression rate and iteration time is optimized.
 */

public class GazelleInvertedIndexHighCardinalityImpl {

	private int[] offsets = null;	
	private int[] data = null;

	private static final Counter invertedDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedDocCount");
	private static final Counter invertedCompressedSize = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedCompressedSize");
	private static final Counter invertedTotalDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedTotalDocCount");

	public void prepData(){

		offsets[0] = 0;

		for(int i = offsets.length - 1; i > 0; i--){
			offsets[i] = offsets[i - 1];
		}

		invertedDocCount.inc(data.length);
		invertedTotalDocCount.inc(data.length);
		invertedCompressedSize.inc(data.length * 4);

	}

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

	public GazelleInvertedHighSet getSet(int dictValue){
		return new GazelleInvertedHighSet(dictValue); 
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

	class GazelleInvertedHighSet extends DocIdSet {

		private int dictValue = 0;

		GazelleInvertedHighSet(int dictValue){
			this.dictValue = dictValue - 1;
		}

		public void addDoc(int id){
			data[offsets[dictValue]++] = id;
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
