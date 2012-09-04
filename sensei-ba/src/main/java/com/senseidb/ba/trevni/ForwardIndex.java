package com.senseidb.ba.trevni;

public interface ForwardIndex {
  int getLength();
  int getValueIndex(int docId);
  /**
   * Used for the facet values, if there are no filtering
   */
  int getFrequency(int valueId);
}
