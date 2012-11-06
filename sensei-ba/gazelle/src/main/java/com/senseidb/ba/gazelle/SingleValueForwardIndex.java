package com.senseidb.ba.gazelle;

public interface SingleValueForwardIndex extends ForwardIndex {
    int getValueIndex(int docId);
}
