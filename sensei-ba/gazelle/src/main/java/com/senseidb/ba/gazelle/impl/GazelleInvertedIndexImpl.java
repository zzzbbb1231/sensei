package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.utils.PForDeltaDocIdSet;

/**
 * Implementation of an InvertedIndex for SenseiBA. We don't store all docIDs in
 * our inverted index; but we opt to keep large 'jumps' and rely on iterating
 * through the forward index for smaller gaps. The size of this jump is calculated
 * so that compression rate and iteration time is optimized.
 */

public class GazelleInvertedIndexImpl extends DocIdSet {

	private static final double THRESHOLD = 0.10;		//Threshold constant that determines how we estimate the jump value

	private PForDeltaDocIdSet PForDSet;			//Internal data set of the DocIDs that we will keep

	private Boolean multiValue = false;			//Is this a Multi Value Iterator?
	private MultiValueForwardIndexImpl1 multiIndex;

	private SingleValueForwardIndex fIndex;
	private SingleValueRandomReader reader;		//Forward iterator needed for sequential iteration
	private int dictValue = 0;					//Value in the dictionary we need for sequential iteration

	private int lastCandidate = 0;				//Last potential candidate that we looked at to add into PForDSet (Could have been added or not)
	private int lastAdded = -1;					//Last DocID that was added to PForDSet
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
			
			//Kamikaze doesn't not support duplicate values in the DocIdSet. If we want to support that (We may end up sacrificing compression for iteration time)
			//We could just change kamikaze's implementation.
			if (lastCandidate == lastAdded) {
				lastCandidate = id;
				totalDocCount++;
				return;
			}

			PForDSet.addDoc(lastCandidate);
			PForDSet.addDoc(id);

			lastCandidate = id;
			lastAdded = id;
			docCount++;
		}

		else {
			lastCandidate = id;
		}

		totalDocCount++;

	}

	/** 
	 * This method figures out the best minimum jump value to use for the inverted indices.
	 * The algorithm is as followed: We go through the forward iterator assuming the optimal jump value is 0.
	 * If at any point, more than THRESHOLD percent of DocIDs are larger than this, we increase the optimal
	 * jump value to be higher than the biggest jump we've seen so far.
	 * 
	 * When we return, we check if the current ratio is less than 5%. This is too little DocIDs in our set
	 * and we would rely too heavily on the forward iterator during next doc. In this case we return the previous optimal
	 * value.
	 * 
	 * This method should /always/ be called before we initialize a column. Otherwise if we don't supply the jump
	 * value into the initializer we could end up spending too much time on estimation.
	 * 
	 * @param forwardIndex -> This is used to iterate through the forward index
	 * @param dictValue -> This is the dict value we are looking for
	 * @return -> The optimal minimum jump value.
	 */
	public static int estimateOptimalMinJump(ForwardIndex forwardIndex, int dictValue) {

		int lastEstimate = 0;
		int currEstimate = 0;
		double currRatio = 0;

		int biggestJump = 0;

		double largerThanEstimate = 0;

		int lastDoc = -1;

		//Things are slightly different when we are dealing with a MultiValueForwardIndex
		if(forwardIndex instanceof MultiValueForwardIndexImpl1){
			double size = forwardIndex.getLength();
			MultiValueForwardIndexImpl1 index = (MultiValueForwardIndexImpl1) forwardIndex;
			int[] buffer = new int[index.getMaxNumValuesPerDoc()];

			for(double i = 0; i < size; i++){
				int count = index.randomRead(buffer, (int) i);
				for(int j = 0; j < count; j++){
					if(buffer[j] == dictValue){
						int jump = (int) (i - lastDoc);
						if(lastDoc == -1){
							lastDoc = (int) i;
						}
						else if(jump > currEstimate){
							largerThanEstimate++;
						}

						if(jump > biggestJump){
							biggestJump = jump;
						}

						lastDoc = (int) i;
					}
				}
				if(i % 1 == 0 && i > 0 && largerThanEstimate/i > THRESHOLD){
					lastEstimate = currEstimate;
					currEstimate = biggestJump + 1;
					currRatio = largerThanEstimate/size;
					largerThanEstimate = 0;
				}
			}
		}
		
		//Same concept applies to SingleValueForwardIndex
		else{
			double size = forwardIndex.getLength();
			SingleValueForwardIndex index = (SingleValueForwardIndex) forwardIndex;
			SingleValueRandomReader ireader = index.getReader();
			for(double i = 0; i < size; i++){
				if(ireader.getValueIndex((int) i) == dictValue){
					int jump = (int) (i - lastDoc);
					if(lastDoc == -1){
						lastDoc = (int) i;
					}
					else if(jump > currEstimate){
						largerThanEstimate++;
					}

					if(jump > biggestJump){
						biggestJump = jump;
					}

					lastDoc = (int) i;
				}
				if(i % 1 == 0 && i > 0 && largerThanEstimate/i > THRESHOLD){
					lastEstimate = currEstimate;
					currEstimate = biggestJump + 1;
					currRatio = largerThanEstimate/size;
					largerThanEstimate = 0;
				}
			}
		}

		//If the current estimate dismisses /too/ many DocIDs, then we'll take the previous estimate
		if(currRatio < 0.05){
			//If the previous estimate is too small, just take 10.
			if(lastEstimate < 10){
				return 10;
			}
			else{
				return (int) lastEstimate;
			}
		}
		else{
			return (int) currEstimate;
		}		

	}

	/** 
	 * Constructor for this class. Creates Kamikaze's PForDelta set, sets the dictValue and figures out the
	 * optimal jump distance
	 * @param forwardIndex -> Needed to fetch the reader
	 * @param dictValue -> Needed for sequential iteration
	 */
	public GazelleInvertedIndexImpl(ForwardIndex forwardIndex, int dictValue, int jumpValue){

		PForDSet = new PForDeltaDocIdSet();

		if(forwardIndex instanceof MultiValueForwardIndexImpl1){
			multiValue = true;
			multiIndex = (MultiValueForwardIndexImpl1) forwardIndex;
		}
		else{
			fIndex = (SingleValueForwardIndex) forwardIndex;
			reader = fIndex.getReader();
		}

		this.dictValue = dictValue;

		//If the jump value is specified (As it should be... Or we may end up taking too much time initializing) we set it.
		//If not, we go estimate it.
		if(jumpValue == 0){
			minJumpValue = estimateOptimalMinJump(forwardIndex, dictValue);
		}
		else{
			minJumpValue = jumpValue;
		}

	}

	/** 
	 * Sequential iteration using the forward index. Used within jumps
	 * @param index -> We grab the first row with the specified value starting with one HIGHER than this index
	 * @return -> The index with the correct dictValue
	 */
	private int getFromForwardIndex(int index) {

		if(multiValue){
			int[] buffer = new int[multiIndex.getMaxNumValuesPerDoc()];
			for (int i = index + 1; i <= finalDoc; i++) {
				int count = multiIndex.randomRead(buffer, i);
				for(int j = 0; j < count; j++){
					if (buffer[j] == dictValue) {
						return i;
					}
				}
			}
		}
		else{
			for (int i = index + 1; i <= finalDoc; i++) {
				if (reader.getValueIndex(i) == dictValue) {
					return i;
				}
			}
		}
		return DocIdSetIterator.NO_MORE_DOCS;
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
		return PForDSet.getCompressedBitSize();
	}

	@Override
	public DocIdSetIterator iterator() throws IOException {
		return new GazelleInvertedIndex();
	}

	/**
	 * This class is used to iterate through the DocID set (Inverted index). The calls work the same way as Lucene's inverted index iterators.
	 * @author jjung
	 *
	 */
	class GazelleInvertedIndex extends DocIdSetIterator {

		private DocIdSetIterator PForDIt;
		private DocIdSetIterator findNextIt;

		private int lastDoc = -1;
		private int currentMin = -1;
		private int lowerBound = -1;

		GazelleInvertedIndex() throws IOException{
			super();

			PForDIt = PForDSet.iterator();
			findNextIt = PForDSet.iterator();

			currentMin = -1;
			lastDoc = -1;
			lowerBound = -1;
			
			if(docCount > 0){
				currentMin = PForDIt.nextDoc();
			}

		}

		@Override
		public int docID() {
			return lastDoc;
		}

		@Override
		public int nextDoc() throws IOException {

			//We exhausted the iterator
			if (lastDoc == DocIdSetIterator.NO_MORE_DOCS) {
				return lastDoc;
			}

			// If we decided to not keep any doc in the inverted iterator or if
			// there are any docs that are between -1 and the current min, return
			// from the forward iterator.
			if (docCount == 0 || lastDoc != currentMin) {
				lastDoc = getFromForwardIndex(lastDoc);
			}

			// Else, we are at a jump, return the higher end of the jump and set all
			// needed variables.
			else if (lastDoc == currentMin) {
				lowerBound = PForDIt.docID();
				lastDoc = PForDIt.nextDoc();
				currentMin = PForDIt.nextDoc();
			}

			return lastDoc;
		}
		
		/**
		 * Helper function used to figure out what value to return when advance is called
		 * @param lowerBound -> We should start iterating from at least this value
		 * @param target -> This is the value that target was called with
		 * @throws IOException -> Comes from kamikaze's API.
		 */

		private int findNext(int lowerBound, int target) throws IOException {
			// This function works as a helper function for advance.

			int i = 1, curr = 0;

			// Move the iterator at least up to the last returned docID
			if (findNextIt.docID() < lowerBound) {
				curr = findNextIt.advance(lowerBound);
			}
			// If the iterator is on the target (The target we're looking for is a
			// valid DocID), return that DocID
			else if (findNextIt.docID() == target) {
				lastDoc = findNextIt.docID();
				return lastDoc;
			}
			// If all else fails, then we have to work through the loop.
			else {
				curr = findNextIt.docID();
			}

			int result = 0;

			// This loop will advance the iterator to the current docid and return
			// the correct docid with respect to how advance is supposed to
			// function.
			while (true) {
				if (target <= curr) {
					if (i % 2 == 1) {
						result = curr;
						currentMin = findNextIt.nextDoc();
					} else {
						result = getFromForwardIndex(target - 1);
						currentMin = curr;
					}

					if (PForDIt.docID() < curr) {
						PForDIt.advance(curr);
					}
					break;

				}
				i++;
				curr = findNextIt.nextDoc();
			}

			return result;
		}

		@Override
		public int advance(int target) throws IOException {

			// Iterator has been exhausted
			if (lastDoc == DocIdSetIterator.NO_MORE_DOCS) {
				return lastDoc;
			}

			// We actually don't hold any DocIDs, we should rely on the forward
			// iterator.
			// (By the way, this is not a good idea; having no DocIDs in the
			// inverted index, nothing will break, but what's the point of having
			// an inverted index then?)
			else if (docCount == 0 || lastDoc == -1) {
				lastDoc = getFromForwardIndex(Math.max(target - 1, lastDoc));
				return lastDoc;
			}

			// If what we're trying to advance to is less than what we're on, return
			// the next element.
			else if (target <= lastDoc) {
				lastDoc = nextDoc();
				return lastDoc;
			}

			//Don't bother with all the cool logic if the target is less than the jump value.
			else if (target - lastDoc <= minJumpValue){
				lastDoc = getFromForwardIndex(target - 1);
				return lastDoc;
			}

			// Okay fine, I guess we'll have to use the helper to find the answer. This is the most expensive option.
			else {
				lastDoc = findNext(lowerBound, target);
			}

			return lastDoc;
		}
	}

}
