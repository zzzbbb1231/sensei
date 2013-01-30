package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.kamikaze.docidset.impl.PForDeltaDocIdSet;

/**
 * Implementation of an InvertedIndex for SenseiBA. We don't store all docIDs in
 * our inverted index; but we opt to keep large 'jumps' and rely on iterating
 * through the forward index for smaller gaps. The size of this jump is calculated
 * so that compression rate and iteration time is optimized.
 */

public class GazelleInvertedIndexImpl extends DocIdSet {

	private static PForDeltaDocIdSet PForDSet;	//Internal data set of the DocIDs that we will keep

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
	 */
	public void addDoc(int id) throws IOException {

		finalDoc = id;

		if (id - lastCandidate > minJumpValue) {

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

	/** This method should return the optimal jump so that compression rate and iteration times are optimized.
	 * Not implemented yet... (I don't know what the algorithm should be.. yet.)
	 * @param forwardIndex -> I though we might need this to read through the forwardIndex)
	 * @return -> The optimal minimum jump value.
	 */
	private int estimateOptimalMinJump(SingleValueForwardIndex forwardIndex) {
		return 6;
	}

	/** 
	 * Constructor for this class. Creates Kamikaze's PForDelta set, sets the dictValue and figures out the
	 * optimial jump distance
	 * @param forwardIndex -> Needed to fetch the reader
	 * @param dictValue -> Needed for sequential iteration
	 */
	public GazelleInvertedIndexImpl(ForwardIndex forwardIndex, int dictValue){

		PForDSet = new PForDeltaDocIdSet();

		SingleValueForwardIndex fIndex = (SingleValueForwardIndex) forwardIndex;
		reader = fIndex.getReader();

		this.dictValue = dictValue;

		minJumpValue = estimateOptimalMinJump(fIndex);

	}

	/** 
	 * Sequetial iteration using the forward index. Used within jumps
	 * @param index -> We grab the first row with the specified value starting with one HIGHER than this index
	 * @return -> The index with the correct dictValue
	 */
	private int getFromForwardIndex(int index) {

		for (int i = index + 1; i <= finalDoc; i++) {
			if (reader.getValueIndex(i) == dictValue) {
				return i;
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
			else if(lastDoc == currentMin) {
				lowerBound = PForDIt.docID();
				lastDoc = PForDIt.nextDoc();
				currentMin = PForDIt.nextDoc();
			}

			return lastDoc;
		}

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

			// Use helper to find the answer
			else {
				lastDoc = findNext(lowerBound, target);
			}
			
			return lastDoc;
		}
	}

}