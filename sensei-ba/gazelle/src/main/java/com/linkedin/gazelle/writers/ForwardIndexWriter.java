package com.linkedin.gazelle.writers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.RandomAccess;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;

/**
 * @author dpatel
 */

public class ForwardIndexWriter {
  
  private static Logger logger = Logger.getLogger(ForwardIndexWriter.class);
  
  private ColumnMedata[] _columnMetadataArr;
  private CompressedIntArray[] _compressedIntArrays;
  private TermValueList[] _termValueListArr;
  private int _length = 0;
  private String _fileName = "gazelle.fIdx";

  public ForwardIndexWriter(ColumnMedata[] columnMetadataArr) {
    _columnMetadataArr = columnMetadataArr;
  }
  
  public ForwardIndexWriter(ColumnMedata[] columnMetadataArr, CompressedIntArray[] compressedIntArrays, TermValueList[] termValueListArr, int length) {
    _columnMetadataArr = columnMetadataArr;
    _compressedIntArrays = compressedIntArrays;
    _termValueListArr = termValueListArr;
    _length = length;
  }

  public void flush(String baseDir) {
    File file = new File(baseDir + "/" + _fileName);
    try {
      RandomAccessFile fIdxFile = new RandomAccessFile(file, "rw");
      long startOffset = 0;
      for (int i = 0; i < _columnMetadataArr.length; i++) {
        int numOfBits = CompressedIntArray.getNumOfBits(_termValueListArr[i].size());
        int bufferSize = CompressedIntArray.getRequiredBufferSize(_length, numOfBits);
        _compressedIntArrays[i].getStorage().rewind();
        fIdxFile.getChannel().write(_compressedIntArrays[i].getStorage(), startOffset);
        fIdxFile.getChannel().force(true);
        startOffset += bufferSize;
      }
      fIdxFile.getChannel().close();
    } catch (FileNotFoundException e) {
      logger.error(e);
    } catch (IOException e) {
      logger.error(e);
    }
  }
}
