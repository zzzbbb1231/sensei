package com.senseidb.ba.gazelle.flushers;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.GazelleUtils;

public class ForwardIndexFlusher {

  public static void flush(HashMap<String, CompressedIntArray> compressedIntArrayMap, HashMap<String, Integer> termValueListSizeMap, int length, String baseDir) throws IOException {
    File file = new File(baseDir, GazelleUtils.INDEX_FILENAME);
    RandomAccessFile fIdxFile = new RandomAccessFile(file, "rw");
    try {
      long startOffset = 0;
      for (String column : compressedIntArrayMap.keySet()) {
        int numOfBits = CompressedIntArray.getNumOfBits(termValueListSizeMap.get(column).intValue());
        int bufferSize = CompressedIntArray.getRequiredBufferSize(length, numOfBits);
        compressedIntArrayMap.get(column).getStorage().rewind();
        fIdxFile.getChannel().write(compressedIntArrayMap.get(column).getStorage(), startOffset);
        fIdxFile.getChannel().force(true);
        startOffset += bufferSize;
      }
    } finally {
      fIdxFile.getChannel().close();
    }
  }
}
