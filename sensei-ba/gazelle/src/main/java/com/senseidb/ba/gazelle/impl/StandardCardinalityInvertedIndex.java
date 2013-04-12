package com.senseidb.ba.gazelle.impl;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;

/**
 * Implementation of an InvertedIndex for SenseiBA. We don't store all docIDs in
 * our inverted index; but we opt to keep large 'jumps' and rely on iterating
 * through the forward index for smaller gaps. The size of this jump is
 * calculated so that compression rate and iteration time is optimized.
 */

public class StandardCardinalityInvertedIndex implements InvertedIndex {

  private final static double THRESHOLD = 0.5;

  private StandardCardinalityInvertedIndexSet[] docIdSets;

  private InvertedIndexStatistics columnInvertedIndexStatistics = new InvertedIndexStatistics();

  /**
   * This method figures out the best minimum jump value to use for the inverted
   * indices. The algorithm is as followed: We go through the forward iterator
   * assuming the optimal jump value is 0. If at any point, more than THRESHOLD
   * percent of DocIDs are larger than this, we increase the optimal jump value
   * to be higher than the biggest jump we've seen so far.
   * 
   * When we return, we check if the current ratio is less than 5%. This is too
   * little DocIDs in our set and we would rely too heavily on the forward
   * iterator during next doc. In this case we return the previous optimal
   * value.
   * 
   * This method should /always/ be called before we initialize a column.
   * Otherwise if we don't supply the jump value into the initializer we could
   * end up spending too much time on estimation.
   * 
   * @param forwardIndex
   *          -> This is used to iterate through the forward index
   * @param dictValue
   *          -> This is the dict value we are looking for
   * @return -> The optimal minimum jump value.
   */
  public static int estimateOptimalMinJump(ForwardIndex forwardIndex, int dictValue) {

    // TODO: Somehow come up with a good algorithm to determine the optimal jump
    // value;
    return 250;

    // if(forwardIndex instanceof MultiValueForwardIndexImpl1){
    // return estimateOptimalMinJumpMulti(forwardIndex, dictValue);
    // }
    // else{
    // return estimateOptimalMinJumpSingle(forwardIndex, dictValue);
    // }

  }

  // Idea remains the same between this one and the multi-value one, so I will
  // only document this method.
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
    SingleValueRandomReader indexReader = index.getReader();
    for (double i = 0; i < size; i++) {
      if (indexReader.getValueIndex((int) i) == dictValue) {

        currCount++;

        if (lastDoc == -1) {
          lastDoc = (int) i;
          continue;
        }

        int jump = (int) (i - lastDoc);

        if (jump > currEstimate) {
          largerThanEstimate++;
        }

        if (jump > biggestJump) {
          biggestJump = jump;
        }

        lastDoc = (int) i;

        if (currCount > 0 && largerThanEstimate / currCount > THRESHOLD) {
          lastEstimate = currEstimate;
          currEstimate = biggestJump + 1;
          currRatio = largerThanEstimate / size;
          largerThanEstimate = 0;
        }
      }
    }

    // If the current estimate dismisses /too/ many DocIDs, then we'll take the
    // previous estimate
    if (currRatio < 0.25) {
      // If the previous estimate is too small, just take 10.
      if (lastEstimate < 250) {
        return 250;
      } else {
        return (int) lastEstimate;
      }
    } else {
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

    for (double i = 0; i < size; i++) {
      int count = index.randomRead(buffer, (int) i);
      for (int j = 0; j < count; j++) {
        if (buffer[j] == dictValue) {

          currCount++;

          if (lastDoc == -1) {
            lastDoc = (int) i;
            continue;
          }

          int jump = (int) (i - lastDoc);

          if (jump > currEstimate) {
            largerThanEstimate++;
          }

          if (jump > biggestJump) {
            biggestJump = jump;
          }

          lastDoc = (int) i;
          if (currCount > 0 && largerThanEstimate / currCount > THRESHOLD) {
            lastEstimate = currEstimate;
            currEstimate = biggestJump + 1;
            currRatio = largerThanEstimate / size;
            largerThanEstimate = 0;
          }
        }
      }
    }

    // If the current estimate dismisses /too/ many DocIDs, then we'll take the
    // previous estimate
    if (currRatio < 0.25) {
      // If the previous estimate is too small, just take 10.
      if (lastEstimate < 250) {
        return 250;
      } else {
        return (int) lastEstimate;
      }
    } else {
      return (int) currEstimate;
    }
  }

  @Override
  public DocIdSet getSet(int dictValue) {
    return docIdSets[dictValue];
  }

  @SuppressWarnings("rawtypes")
  public StandardCardinalityInvertedIndex(ForwardIndex fIndex, int size, int optVal, TermValueList values) throws IOException {
    docIdSets = new StandardCardinalityInvertedIndexSet[size];

    for (int i = 0; i < size; i++) {
      String value = values.get(i);
      docIdSets[i] = new StandardCardinalityInvertedIndexSet(fIndex, fIndex.getDictionary().indexOf(value), optVal);
    }
  }

  public void addDoc(int id, int value) throws IOException {
    docIdSets[value].addDoc(id);
  }

  /**
   * This function update the statistics information of the inverted index. This
   * MUST be called after the initializer.
   */
  public void updateStatisticsInfo() {
    long invertedIndexDocCount = 0;
    long invertedIndexTrueDocCount = 0;
    long invertedIndexCompressedSize = 0;
    for (int i = 0; i < docIdSets.length; ++i) {
      invertedIndexDocCount += docIdSets[i].getCount();
      invertedIndexTrueDocCount += docIdSets[i].getTrueCount();
      invertedIndexCompressedSize += docIdSets[i].getCompSize();
    }
    invertedIndexCompressedSize /= 8;
    columnInvertedIndexStatistics.setDocCount(invertedIndexDocCount);
    columnInvertedIndexStatistics.setTrueDocCount(invertedIndexTrueDocCount);
    columnInvertedIndexStatistics.setCompressedSize(invertedIndexCompressedSize);
    columnInvertedIndexStatistics.setInvertedIndexStrategy("StandardCardinality");
  }

  @Override
  public boolean invertedIndexPresent(int index) {
    return docIdSets[index] != null;
  }

  /**
   * Flushes out the uncompressed block in the PForDelta sets; saving more
   * memory space.
   * 
   * @throws IOException
   */
  public void flush() throws IOException {
    for (int i = 0; i < docIdSets.length; i++) {
      if (docIdSets[i] != null) {
        docIdSets[i].flush();
      }
    }
  }

  /**
   * Seals off any extra space in arrays, to save more space.
   */
  public void optimize() {
    for (int i = 0; i < docIdSets.length; i++) {
      if (docIdSets[i] != null) {
        docIdSets[i].optimize();
      }
    }
    updateStatisticsInfo();
  }

  @Override
  public int length() {
    return docIdSets.length;
  }

  @Override
  public InvertedIndexStatistics getIndexStatistics() {
    return columnInvertedIndexStatistics;
  }
}