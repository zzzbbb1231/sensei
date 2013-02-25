package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.impl.FacetUtils.ForwardDocIdSet;
import com.senseidb.ba.gazelle.impl.FacetUtils.ForwardIndexIterator;
import com.senseidb.ba.gazelle.utils.PForDeltaDocIdSet;

public class StandardCardinalityInvertedIndexSet extends DocIdSet {

	ForwardIndexReader indexReader;					//Used when we call getFromForwardIndex
	ForwardIndex forwardIndex;

	private PForDeltaDocIdSet pForDSet;		//Internal data set of the DocIDs that we will keep

	protected MultiValueForwardIndex multiIndex;	//Used to hold the MultiValueForwardIndex reader
	protected int[] buffer;					//Used for reading from the MultiValue reader

	protected GazelleForwardIndexImpl gazelleForwardIndex;
	protected ForwardIndexIterator forwardIndexReader;		//Forward iterator needed for sequential iteration
	protected int dictValue = 0;					//Value in the dictionary we need for sequential iteration

	private int lastCandidate = -1;				//Last potential candidate that we looked at to add into PForDSet (Could have been added or not)
	private int docCount = 0;					//Number of docs that we keep in the set
	private int totalDocCount = 0;				//Actual docs with the specified value
	private int finalDoc = -1;					//The very last doc with the specified value (Doesn't have to be added into the PForDSet)

	private int minJumpValue = 0;				//If two DocIDs are at least this far apart, we add it to PForDSet.


	/** 
	 * Used to add DocIDs to the set. Calling this method doesn't guarantee that the DocIDs that you are 
	 * trying to add will be stored in the set's data structure. In most cases, do not call this method after fetching an iterator.
	 * @param id -> The DocID you want to add to the set.
	 * @throws IOException -> Comes from Kamikaze's PForDelta implementation
	 * @throws InterruptedException 
	 */
	public void addDoc(int id) throws IOException {		

		finalDoc = id;

		if (id - lastCandidate > minJumpValue) {

			pForDSet.addDoc(lastCandidate + 1);
			pForDSet.addDoc(id + 1);

			lastCandidate = id;
			docCount++;
		}

		else {
			lastCandidate = id;
		}
		totalDocCount++;

	}

	/** 
	 * Constructor for this class. Creates Kamikaze's PForDelta set, sets the dictValue and figures out the
	 * optimal jump distance
	 * @param forwardIndex -> Needed to fetch the reader
	 * @param dictValue -> Needed for sequential iteration
	 * @throws IOException 
	 */
	public StandardCardinalityInvertedIndexSet(ForwardIndex forwardIndex, int dictValue, int jumpValue) throws IOException{
		
		this.dictValue = dictValue;
		this.forwardIndex = forwardIndex;

		//If the jump value is specified (As it should be... Or we may end up taking too much time initializing) we set it.
		//If not, we go estimate it.
		if(jumpValue == 0){
			minJumpValue = StandardCardinalityInvertedIndex.estimateOptimalMinJump(forwardIndex, dictValue);
		}
		else{
			minJumpValue = jumpValue;
		}
		
		pForDSet = new PForDeltaDocIdSet();

	}

	/**
	 * @return -> Total number of documents with dictValue (This doesn't mean we necessarily keep them all in this class)
	 */
	public int getCount() {
		return totalDocCount;
	}

	/**
	 * @return -> Total number of documents that are ACTUALLY being stored in this class
	 */
	public int getTrueCount() {
		return docCount;
	}

	/**
	 * @return -> Size (In bits) of the compressed ints. This does not include any other variables we hold in this class.
	 */
	public long getCompSize() {
		return pForDSet.getCompressedBitSize();
	}
	
	public void optimize(){
		pForDSet.optimize();
	}
	
	public void flush() throws IOException{
		pForDSet.flush(0);
	}

	@Override
	public DocIdSetIterator iterator() throws IOException {
		return new GazelleInvertedIndexIterator();
	}

	/**
	 * An interface (With two implementations) created to deal with branching when we call getFromForwardIndex.
	 * @author jjung
	 *
	 */

	private interface ForwardIndexReader{

		/** 
		 * Sequential iteration using the forward index. Used within jumps
		 * @param index -> We grab the first row with the specified value starting with one HIGHER than this index
		 * @return -> The index with the correct dictValue
		 * @throws IOException 
		 */		
		int getFromForwardIndex(int index) throws IOException;

	}

	class SingleValueForwardIndexReader implements ForwardIndexReader{

		public SingleValueForwardIndexReader(ForwardIndex forwardIndex, int dictValue) throws IOException {
			gazelleForwardIndex = (GazelleForwardIndexImpl) forwardIndex;
			ForwardDocIdSet docIdSet = new FacetUtils.ForwardDocIdSet((GazelleForwardIndexImpl) forwardIndex, dictValue, finalDoc);
			forwardIndexReader = (ForwardIndexIterator) docIdSet.iterator();
		}

		public int getFromForwardIndex(int index) throws IOException {
			if(index > finalDoc){
				return DocIdSetIterator.NO_MORE_DOCS;
			}
			else{
				return forwardIndexReader.advance(index);
			}
		}

	}

	class MultiValueForwardIndexReader implements ForwardIndexReader{

		public MultiValueForwardIndexReader(ForwardIndex forwardIndex, int dictValue) {
			multiIndex = (MultiValueForwardIndex) forwardIndex;
			buffer = new int[multiIndex.getMaxNumValuesPerDoc()];
		}

		public int getFromForwardIndex(int index) {

			for (int i = index + 1; i <= finalDoc; i++) {
				int count = multiIndex.randomRead(buffer, i);
				for(int j = 0; j < count; j++){
					if (buffer[j] == dictValue) {
						return i;
					}
				}
			}

			return DocIdSetIterator.NO_MORE_DOCS;
		}

	}

	/**
	 * This class is used to iterate through the DocID set (Inverted index). The calls work the same way as Lucene's inverted index iterators.
	 * @author jjung
	 *
	 */
	class GazelleInvertedIndexIterator extends DocIdSetIterator {

		private DocIdSetIterator pForDIt;

		private int lastDoc = -1;
		private int currentMin = -1;

		GazelleInvertedIndexIterator() throws IOException{
			super();
			
			if(forwardIndex instanceof MultiValueForwardIndex){
				indexReader = new MultiValueForwardIndexReader(forwardIndex, dictValue);
			}
			else{
				indexReader = new SingleValueForwardIndexReader(forwardIndex, dictValue);
			}
			
			StandardCardinalityInvertedIndex.invertedTotalDocCount.inc(getCount());
			StandardCardinalityInvertedIndex.invertedDocCount.inc(getTrueCount());
			StandardCardinalityInvertedIndex.invertedCompressedSize.inc(getCompSize());
			
			pForDIt = pForDSet.iterator();

			currentMin = -1;
			lastDoc = -1;

			if(docCount > 0){
				currentMin = pForDIt.nextDoc() - 1;
			}

		}

		@Override
		public int docID() {
			return lastDoc;
		}

		@Override
		public int nextDoc() throws IOException {

			//We exhausted the iterator
			if (lastDoc >= DocIdSetIterator.NO_MORE_DOCS - 1 || lastDoc >= finalDoc) {
				lastDoc = DocIdSetIterator.NO_MORE_DOCS;
				return lastDoc;
			}

			// If we decided to not keep any doc in the inverted iterator or if
			// there are any docs that are between -1 and the current min, return
			// from the forward iterator.
			if (docCount == 0 || lastDoc != currentMin){
				lastDoc = indexReader.getFromForwardIndex(lastDoc + 1);
			}

			// Else, we are at a jump, return the higher end of the jump and set all
			// needed variables.
			else if (lastDoc == currentMin) {
				lastDoc = pForDIt.nextDoc() - 1;
				currentMin = pForDIt.nextDoc() - 1;
			}

			return lastDoc;
		}

		/**
		 * Helper function used to figure out what value to return when advance is called
		 * @param lowerBound -> We should start iterating from at least this value
		 * @param target -> This is the value that target was called with
		 * @throws IOException -> Comes from kamikaze's API.
		 */

		private int findNext(int target) throws IOException {
			// This function works as a helper function for advance.

			int curr = pForDIt.advance(target + 1) - 1;
		
			int result = 0;
			
			if(curr == DocIdSetIterator.NO_MORE_DOCS){
				result = indexReader.getFromForwardIndex(target); 
			}
			else if(pForDSet.jump){
				result = curr;
				currentMin = pForDIt.nextDoc() - 1;				
			}
			else{
				result = indexReader.getFromForwardIndex(target);
				currentMin = curr;
			}

			return result;
		}

		@Override
		public int advance(int target) throws IOException {

			if (lastDoc == DocIdSetIterator.NO_MORE_DOCS || target >= finalDoc) {
				lastDoc = DocIdSetIterator.NO_MORE_DOCS;
				return lastDoc;
			}

			// If what we're trying to advance to is less than what we're on, return
			// the next element.
			else if (target <= lastDoc) {
				lastDoc = nextDoc();
			}

			//Don't bother with all the cool logic if we don't hold any docs in our class.
			else if (docCount == 0 || target <= currentMin){
				lastDoc = indexReader.getFromForwardIndex(target);
			}

			// Okay fine, I guess we'll have to use the helper to find the answer. This is the most expensive option.
			else {
				lastDoc = findNext(target);
			}

			return lastDoc;
		}
	}

}
