package com.senseidb.ba;

public interface SingleValueForwardIndex extends ForwardIndex {
    int getValueIndex(int docId);
}
