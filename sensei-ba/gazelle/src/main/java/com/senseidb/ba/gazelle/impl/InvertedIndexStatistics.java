package com.senseidb.ba.gazelle.impl;

import java.io.Serializable;

public class InvertedIndexStatistics implements Serializable {
  private long docCount = 0;
  private long trueDocCount = 0;
  private long compressedSize = 0;
  private String invertedIndexStrategy = "Skipped";

  public InvertedIndexStatistics() {
    docCount = 0;
    trueDocCount = 0;
    compressedSize = 0;
    invertedIndexStrategy = "Skipped";
  }

  public void incrementStatisticsCount(InvertedIndexStatistics indexStatistics) {
    docCount += indexStatistics.getDocCount();
    trueDocCount += indexStatistics.getTrueDocCount();
    compressedSize += indexStatistics.getCompressedSize();
  }

  public void setDocCount(long docCount) {
    this.docCount = docCount;
  }

  public void setTrueDocCount(long trueDocCount) {
    this.trueDocCount = trueDocCount;
  }

  public void setCompressedSize(long compressedSize) {
    this.compressedSize = compressedSize;
  }

  public void setInvertedIndexStrategy(String invertedIndexStrategy) {
    this.invertedIndexStrategy = invertedIndexStrategy;
  }

  public long getDocCount() {
    return docCount;
  }

  public long getTrueDocCount() {
    return trueDocCount;
  }

  public long getCompressedSize() {
    return compressedSize;
  }

  public String getInvertedIndexStrategy() {
    return invertedIndexStrategy;
  }
}
