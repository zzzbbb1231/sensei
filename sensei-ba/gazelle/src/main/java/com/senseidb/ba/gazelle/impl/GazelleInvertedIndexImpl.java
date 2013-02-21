package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.impl.FacetUtils.ForwardDocIdSet;
import com.senseidb.ba.gazelle.impl.FacetUtils.ForwardIndexIterator;
import com.senseidb.ba.gazelle.utils.PForDeltaDocIdSet;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

/**
 * Implementation of an InvertedIndex for SenseiBA. We don't store all docIDs in
 * our inverted index; but we opt to keep large 'jumps' and rely on iterating
 * through the forward index for smaller gaps. The size of this jump is calculated
 * so that compression rate and iteration time is optimized.
 */

public class GazelleInvertedIndexImpl extends DocIdSet {

	private final static double THRESHOLD = 0.70;

	ForwardIndexReader iIndex;					//Used when we call getFromForwardIndex

	private PForDeltaDocIdSet pForDSet;		//Internal data set of the DocIDs that we will keep

	protected MultiValueForwardIndex multiIndex;	//Used to hold the MultiValueForwardIndex reader
	protected int[] buffer;					//Used for reading from the MultiValue reader

	protected GazelleForwardIndexImpl fIndex;
	protected ForwardIndexIterator reader;		//Forward iterator needed for sequential iteration
	protected int dictValue = 0;					//Value in the dictionary we need for sequential iteration

	private int lastCandidate = -1;				//Last potential candidate that we looked at to add into PForDSet (Could have been added or not)
	private int docCount = 0;					//Number of docs that we keep in the set
	private int totalDocCount = 0;				//Actual docs with the specified value
	private int finalDoc = -1;					//The very last doc with the specified value (Doesn't have to be added into the PForDSet)

	private int minJumpValue = 0;				//If two DocIDs are at least this far apart, we add it to PForDSet.
	
	private static final Counter invertedDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedDocCount");
	private static final Counter invertedCompressedSize = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedCompressedSize");
	private static final Counter invertedTotalDocCount = Metrics.newCounter(GazelleInvertedIndexImpl.class, "invertedTotalDocCount");


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

		if(forwardIndex instanceof MultiValueForwardIndexImpl1){
			return estimateOptimalMinJumpMulti(forwardIndex, dictValue);
		}
		else{
			return estimateOptimalMinJumpSingle(forwardIndex, dictValue);
		}

	}

	//Idea remains the same between this one and the multi-value one, so I will only document this method.
	public static int estimateOptimalMinJumpSingle(ForwardIndex forwardIndex, int dictValue) {

		int lastEstimate = 0;
		int currEstimate = 0;
		double currRatio = 0;

		int biggestJump = 0;

		double largerThanEstimate = 0;

		int lastDoc = -1;
		int currCount = 0;

		double size = forwardIndex.getLength();
		SingleValueForwardIndex index = (SingleValueForwardIndex) forwardIndex;
		SingleValueRandomReader ireader = index.getReader();
		for(double i = 0; i < size; i++){
			if(ireader.getValueIndex((int) i) == dictValue){
				
				currCount++;
				
				if(lastDoc == -1){
					lastDoc = (int) i;
					continue;
				}
				
				int jump = (int) (i - lastDoc);
				
				if(jump > currEstimate){
					largerThanEstimate++;
				}

				if(jump > biggestJump){
					biggestJump = jump;
				}

				lastDoc = (int) i;
				
				if(currCount > 0 && largerThanEstimate/currCount > THRESHOLD){
					lastEstimate = currEstimate;
					currEstimate = biggestJump + 1;
					currRatio = largerThanEstimate/size;
					largerThanEstimate = 0;
				}
			}
		}

		//If the current estimate dismisses /too/ many DocIDs, then we'll take the previous estimate
		if(currRatio < 0.25){
			//If the previous estimate is too small, just take 10.
			if(lastEstimate < 250){
				return 250;
			}
			else{
				return (int) lastEstimate;
			}
		}
		else{
			return (int) currEstimate;
		}	

	}

	public static int estimateOptimalMinJumpMulti(ForwardIndex forwardIndex, int dictValue) {

		int lastEstimate = 0;
		int currEstimate = 0;
		double currRatio = 0;

		int biggestJump = 0;

		double largerThanEstimate = 0;

		int lastDoc = -1;
		int currCount = 0;

		double size = forwardIndex.getLength();
		MultiValueForwardIndexImpl1 index = (MultiValueForwardIndexImpl1) forwardIndex;
		int[] buffer = new int[index.getMaxNumValuesPerDoc()];

		for(double i = 0; i < size; i++){
			int count = index.randomRead(buffer, (int) i);
			for(int j = 0; j < count; j++){
				if(buffer[j] == dictValue){
					
					currCount++;
					
					if(lastDoc == -1){
						lastDoc = (int) i;
						continue;
					}
					
					int jump = (int) (i - lastDoc);
					
					if(jump > currEstimate){
						largerThanEstimate++;
					}

					if(jump > biggestJump){
						biggestJump = jump;
					}

					lastDoc = (int) i;
					if(currCount > 0 && largerThanEstimate/currCount > THRESHOLD){
						lastEstimate = currEstimate;
						currEstimate = biggestJump + 1;
						currRatio = largerThanEstimate/size;
						largerThanEstimate = 0;
					}
				}
			}
		}

		//If the current estimate dismisses /too/ many DocIDs, then we'll take the previous estimate
		if(currRatio < 0.25){
			//If the previous estimate is too small, just take 10.
			if(lastEstimate < 100){
				return 100;
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
	 * @throws IOException 
	 */
	public GazelleInvertedIndexImpl(ForwardIndex forwardIndex, int dictValue, int jumpValue) throws IOException{
		
		this.dictValue = dictValue;

		if(forwardIndex instanceof MultiValueForwardIndex){
			iIndex = new MultiValueForwardIndexReader(forwardIndex, dictValue, jumpValue);
		}
		else{
			iIndex = new SingleValueForwardIndexReader(forwardIndex, dictValue, jumpValue);
		}

		//If the jump value is specified (As it should be... Or we may end up taking too much time initializing) we set it.
		//If not, we go estimate it.
		if(jumpValue == 0){
			minJumpValue = estimateOptimalMinJump(forwardIndex, dictValue);
		}
		else{
			minJumpValue = jumpValue;
		}
		
		//TODO: Change this so we can get a general jump value
		minJumpValue = 250;
		
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
	
	public static long getTotalCount() {
		return invertedTotalDocCount.count();
	}

	public static long getTotalTrueCount() {
		return invertedDocCount.count();
	}

	public static long getTotalCompSize() {
		return invertedCompressedSize.count();
	}
	
	public void optimize(){
		pForDSet.optimize();
	}
	
	public void flush() throws IOException{
		pForDSet.flush(0);
	}

	@Override
	public DocIdSetIterator iterator() throws IOException {
		return new GazelleInvertedIndex();
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

		public SingleValueForwardIndexReader(ForwardIndex forwardIndex, int dictValue, int jumpValue) throws IOException {
			fIndex = (GazelleForwardIndexImpl) forwardIndex;
			ForwardDocIdSet asdf = new FacetUtils.ForwardDocIdSet((GazelleForwardIndexImpl) forwardIndex, dictValue);
			reader = (ForwardIndexIterator) asdf.iterator();
		}

		public int getFromForwardIndex(int index) throws IOException {
			if(index > finalDoc){
				return DocIdSetIterator.NO_MORE_DOCS;
			}
			else{
				return reader.advance(index);
			}
		}

	}

	class MultiValueForwardIndexReader implements ForwardIndexReader{

		public MultiValueForwardIndexReader(ForwardIndex forwardIndex, int dictValue, int jumpValue) {
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
	class GazelleInvertedIndex extends DocIdSetIterator {

		private DocIdSetIterator PForDIt;

		private int lastDoc = -1;
		private int currentMin = -1;

		GazelleInvertedIndex() throws IOException{
			super();
			
			invertedTotalDocCount.inc(getCount());
			invertedDocCount.inc(getTrueCount());
			invertedCompressedSize.inc(getCompSize());
			
			PForDIt = pForDSet.iterator();

			currentMin = -1;
			lastDoc = -1;

			if(docCount > 0){
				currentMin = PForDIt.nextDoc() - 1;
			}

		}

		@Override
		public int docID() {
			return lastDoc;
		}

		@Override
		public int nextDoc() throws IOException {

			//We exhausted the iterator
			if (lastDoc == DocIdSetIterator.NO_MORE_DOCS || lastDoc >= finalDoc) {
				lastDoc = DocIdSetIterator.NO_MORE_DOCS;
				return lastDoc;
			}

			// If we decided to not keep any doc in the inverted iterator or if
			// there are any docs that are between -1 and the current min, return
			// from the forward iterator.
			if (docCount == 0 || lastDoc != currentMin){
				lastDoc = iIndex.getFromForwardIndex(lastDoc + 1);
			}

			// Else, we are at a jump, return the higher end of the jump and set all
			// needed variables.
			else if (lastDoc == currentMin) {
				lastDoc = PForDIt.nextDoc() - 1;
				currentMin = PForDIt.nextDoc() - 1;
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

			int curr = PForDIt.advance(target) - 1;
		
			int result = 0;
			
			if(curr == DocIdSetIterator.NO_MORE_DOCS){
				result = iIndex.getFromForwardIndex(target); 
			}
			else if(pForDSet.jump){
				result = curr;
				currentMin = PForDIt.nextDoc() - 1;				
			}
			else{
				result = iIndex.getFromForwardIndex(target);
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
				lastDoc = iIndex.getFromForwardIndex(target);
			}

			// Okay fine, I guess we'll have to use the helper to find the answer. This is the most expensive option.
			else {
				lastDoc = findNext(target);
			}

			return lastDoc;
		}
	}

}
