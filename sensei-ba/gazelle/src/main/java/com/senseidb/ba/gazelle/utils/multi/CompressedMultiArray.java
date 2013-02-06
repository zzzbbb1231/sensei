package com.senseidb.ba.gazelle.utils.multi;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.util.OpenBitSet;

import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class CompressedMultiArray { 
    private CompressedMultiArrayChunk currentChunk;
    private List<CompressedMultiArrayChunk> chunks = new ArrayList<CompressedMultiArrayChunk>();
    private int numberOfElements = 0;   
    int maxNumOfElementsPerChunk;
    private final int numBitsPerElement;
    private int initialSize;
    private CompressedMultiArrayChunk[] chunksArr;
    private int maxNumValuesPerDoc;
    private final boolean isOffHeap;    
    private CompressedMultiArray(int numBitsPerElement) {
      this.numBitsPerElement = numBitsPerElement;
      isOffHeap = false;
     
    }
    public CompressedMultiArray(int numBitsPerElement, int initialSize, boolean isOffHeap) {
      this.numBitsPerElement = numBitsPerElement;
      this.initialSize = initialSize;
      this.isOffHeap = isOffHeap;     
      currentChunk = new CompressedMultiArrayChunk(0, numBitsPerElement, initialSize, isOffHeap);
      chunks.add(currentChunk);
      maxNumOfElementsPerChunk = Integer.MAX_VALUE / numBitsPerElement;
    }
    public void add(int[] values, int length) {      
      if (currentChunk.getCurrentSize() + length > maxNumOfElementsPerChunk) {
        currentChunk = new CompressedMultiArrayChunk(numberOfElements, numBitsPerElement, initialSize, isOffHeap);
        chunks.add(currentChunk);      
      }
      currentChunk.add(values, length);
      numberOfElements++;
    }
    public void add(int... values) {      
     add(values, values.length);
    }
    public void initSkipLists() {
      for (CompressedMultiArrayChunk arrayChunk : chunks) {
        arrayChunk.initSkipLists();
      }
      chunksArr = chunks.toArray(new CompressedMultiArrayChunk[chunks.size()]);
      initMaxNumValuesPerDoc(chunksArr);
    }
    private void initMaxNumValuesPerDoc(CompressedMultiArrayChunk[] chunksArr) {
        maxNumValuesPerDoc = 0;
       for (CompressedMultiArrayChunk compressedMultiArrayChunk : chunksArr) {
           maxNumValuesPerDoc = Math.max(maxNumValuesPerDoc, compressedMultiArrayChunk.getMaxNumValuesPerDoc());
       }
  }
    public void flushToFile(File dir, String columnName) {
      for (int i = 0; i < chunks.size(); i++) {
        File file = new File(dir, columnName + ".index.part" + String.format("%05d", i));
        try {
          file.createNewFile();
        } catch (IOException e) {
         throw new RuntimeException(e);
        }
        chunks.get(i).flush(file);
      }
    }
    public void flushToFile(String dir, String columnName, FileSystemMode fileSystemMode, FileSystem fileSystem) {
        for (int i = 0; i < chunks.size(); i++) {
            DataOutputStream outputStream = null;
            try {
             outputStream = StreamUtils.getOutputStream(dir + "/" + columnName + ".index.part" + String.format("%05d", i), fileSystemMode, fileSystem);
            
          chunks.get(i).flush(outputStream);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }finally {
            IOUtils.closeQuietly(outputStream);}
        }
      }
    public static CompressedMultiArray readFromFile(File dir, final String columnName, int numBitsPerElement,ReadMode readMode) {
      CompressedMultiArray compressedMultiArray = new CompressedMultiArray(numBitsPerElement);
      String[] fileNames = dir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(columnName + ".index.part");
        }
      });
      Arrays.sort(fileNames);
      for (String fileName : fileNames) {
        File file = new File(dir, fileName);
        compressedMultiArray.chunks.add(CompressedMultiArrayChunk.readFromFile(numBitsPerElement, file, readMode));
      }
      compressedMultiArray.initSkipLists();
      return compressedMultiArray;
    }
    public int getMaxNumOfElementsPerChunk() {
      return maxNumOfElementsPerChunk;
    }
    public void setMaxNumOfElementsPerChunk(int maxNumOfElementsPerChunk) {
      this.maxNumOfElementsPerChunk = maxNumOfElementsPerChunk;
    }
    public MultiFacetIterator iterator() {
      if (chunks.size() == 1) {
        return chunks.get(0).iterator();
      }
      MultiChunkIterator[] iterators = new MultiChunkIterator[chunks.size()];
      for (int i = 0; i < chunks.size(); i++) {
        iterators[i] = chunks.get(i).iterator();
      }
      return new CompositeMultiFacetIterator(iterators);
    }

    public int randomRead(int[] buffer, int index) {
        if (chunksArr.length == 1) {
            return chunksArr[0].randomRead(buffer, index);
        }
        int currentIndex = 0;
        while (currentIndex < chunksArr.length - 1) {
            if (chunksArr[currentIndex + 1].getStartElement() > index) {
                break;
            }
            currentIndex++;
        }
        if (currentIndex >= chunksArr.length) {
            return 0;
        }
        return chunksArr[currentIndex].randomRead(buffer, index);
    }
    public int getMaxNumValuesPerDoc() {
        return maxNumValuesPerDoc;
    }
}
