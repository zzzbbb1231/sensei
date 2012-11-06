package com.senseidb.ba.gazelle.utils.multi;

import com.browseengine.bobo.util.BigSegmentedArray;

public class CompositeMultiFacetIterator implements MultiFacetIterator {
  private MultiChunkIterator currentIterator;
  private int currentIndex;
  private final MultiChunkIterator[] iterators;

  public CompositeMultiFacetIterator(MultiChunkIterator[] iterators) {
    this.iterators = iterators;
    currentIterator = iterators[0];
    currentIndex = 0;
  }

  @Override
  public boolean advance(int index) {
    boolean res = currentIterator.advance(index);
    if (!res) {
      while (currentIndex < iterators.length - 1) {
        if (iterators[currentIndex + 1].getStartElement() > index) {
          break;
        }
        currentIndex++;
      }
      if (currentIndex >= iterators.length) {
        return false;
      }
      currentIterator = iterators[currentIndex];
      return currentIterator.advance(index);
    }
    return true;
  }

  @Override
  public int readValues(int[] buffer) {
    return currentIterator.readValues(buffer);
  }

  @Override
  public int find(int fromIndex, int value) {
    if (!advance(fromIndex)) {
      return -1;
    }
    int ret = currentIterator.find(fromIndex, value);
    if (ret >= 0) {
      return ret;
    }
    while (++currentIndex < iterators.length) {
      currentIterator = iterators[currentIndex];
      fromIndex = Math.max(currentIterator.getStartElement(), fromIndex);
      ret = currentIterator.find(fromIndex, value);
      if (ret >= 0) {
        return ret;
      }
    }
    return -1;
  }

  @Override
  public void count(BigSegmentedArray counts) {
    currentIterator.count(counts);
  }

  @Override
  public int find(int fromIndex, int startIndex, int endIndex) {
    if (!advance(fromIndex)) {
      return -1;
    }
    int ret = currentIterator.find(fromIndex, startIndex, endIndex);
    if (ret >= 0) {
      return ret;
    }
    while (++currentIndex < iterators.length) {
      currentIterator = iterators[currentIndex];
      fromIndex = Math.max(currentIterator.getStartElement(), fromIndex);
      ret = currentIterator.find(fromIndex, startIndex, endIndex);
      if (ret >= 0) {
        return ret;
      }
    }
    return -1;
  }

}
