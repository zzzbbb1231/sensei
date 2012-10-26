package com.senseidb.ba.gazelle.creators;

import com.browseengine.bobo.util.BigNestedIntArray;
import com.browseengine.bobo.util.BigNestedIntArray.BufferedLoader;
import com.browseengine.bobo.util.BigSegmentedArray;

public class IndexedRecord {
  public int size() {return 0;}
  
  public Object getValue(int index) {return null;}
  
  public static void main(String[] args) throws Exception {
    BufferedLoader bufferedLoader = new BufferedLoader(1000 + 1);
    BigNestedIntArray array = new  BigNestedIntArray();
    for (int i = 0; i < 1000; i ++) {
      bufferedLoader.add(i, 1);
      bufferedLoader.add(i, 2);
      bufferedLoader.add(i, 3);
    }
    array.load(1000 + 1, bufferedLoader);
    for (int i = 0; i < 1000; i ++) {
      System.out.println(array.getNumItems(i));
    }
  }
}
