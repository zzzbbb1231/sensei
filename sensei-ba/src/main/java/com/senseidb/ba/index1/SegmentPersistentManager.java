package com.senseidb.ba.index1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.util.CompressedIntArray;

public class SegmentPersistentManager {
	private static final String METADATA_PROPERTIES = "metadata.properties";
	private static final String INDEX_FILE_NAME = "index.blob";
	public void persist(File directory, IndexSegmentImpl indexSegmentImpl) throws Exception {
		directory.mkdirs();
		File forwardIndexStorage = new File(directory, INDEX_FILE_NAME);
		RandomAccessFile forwardIndexFile = new RandomAccessFile(forwardIndexStorage, "rw");
		PropertiesConfiguration propertiesConfiguration = getPropertiesMetadata(directory);
		for (String column : indexSegmentImpl.getColumnTypes().keySet()) {
			ForwardIndexImpl forwardIndex = (ForwardIndexImpl) indexSegmentImpl.getForwardIndex(column);
			forwardIndex.getColumnMetadata().save(propertiesConfiguration);
			File dictionaryFile = getDictionaryFile(directory, column);
			OutputStream outputStream = new FileOutputStream(dictionaryFile);
			DictionaryPersistentManager.persist(outputStream, forwardIndex.getDictionary());
			outputStream.close();
			forwardIndexFile.getChannel().write(forwardIndex.getCompressedIntArray().getStorage(), forwardIndex.getColumnMetadata().getStartOffset());
			forwardIndexFile.getChannel().force(true);
		}
		propertiesConfiguration.save();
		forwardIndexFile.getChannel().close();
	}
	public IndexSegmentImpl read(File directory, boolean memoryMappedMode) throws Exception {
		RandomAccessFile forwardIndexFile = null;
		try {
    		IndexMetadata indexMetadata = IndexMetadata.readFromConfiguration(getPropertiesMetadata(directory));
    		IndexSegmentImpl indexSegmentImpl = new IndexSegmentImpl();
    		
    		File forwardIndexStorage = new File(directory, INDEX_FILE_NAME);
    	    forwardIndexFile = new RandomAccessFile(forwardIndexStorage, "r");
    		for(String column : indexMetadata.getMetadata().keySet()) {
    		    InputStream inputStream = null;
    		    try {ColumnMetadata columnMetadata = indexMetadata.getMetadata().get(column);
        			indexSegmentImpl.getColumnTypes().put(column, columnMetadata.getColumnType());
        			File dictionaryFile = getDictionaryFile(directory, column);
        			 inputStream = new FileInputStream(dictionaryFile);
        			TermValueList dictionary = DictionaryPersistentManager.read(inputStream, columnMetadata.getNumberOfDictionaryValues(), columnMetadata.getColumnType());
        			indexSegmentImpl.getDictionaries().put(column, dictionary);
        			inputStream.close();
        			ByteBuffer byteBuffer = null;
        			if (memoryMappedMode) {
        				forwardIndexFile.getChannel().map(MapMode.READ_ONLY, columnMetadata.getStartOffset(), columnMetadata.getByteLength());
        			} else {
        				byteBuffer =  ByteBuffer.allocateDirect((int)columnMetadata.getByteLength());
        				forwardIndexFile.getChannel().read(byteBuffer, columnMetadata.getStartOffset());
        			}
        			CompressedIntArray compressedIntArray = new CompressedIntArray(columnMetadata.getNumberOfElements(), CompressedIntArray.getNumOfBits(columnMetadata.getNumberOfDictionaryValues()), byteBuffer); 
        			indexSegmentImpl.getForwardIndexes().put(column, new ForwardIndexImpl(column, compressedIntArray, dictionary, columnMetadata));
    		    } finally {
    		        IOUtils.closeQuietly(inputStream);
    		    }
    		}
    		return indexSegmentImpl;
		} finally {
    		if (!memoryMappedMode && forwardIndexFile != null) {
    			forwardIndexFile.close();
    		}
		}
		
	}
    public PropertiesConfiguration getPropertiesMetadata(File directory) throws ConfigurationException {
        return new PropertiesConfiguration(new File(directory, METADATA_PROPERTIES));
    }
	private File getDictionaryFile(File directory, String column) {
		return new File(directory, column + ".dict");
	}
}
