package com.senseidb.ba.index1;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.senseidb.ba.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.flushers.SegmentFlusher;
import com.senseidb.ba.gazelle.readers.SegmentReader;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class SegmentPersistentManager {
    private static Logger logger = Logger.getLogger(SegmentPersistentManager.class);
    
    public static final String METADATA_PROPERTIES = "metadata.properties";
	public static final String INDEX_FILE_NAME = "index.blob";
	public static void persist(File directory, GazelleIndexSegmentImpl indexSegmentImpl) throws Exception {
		SegmentFlusher.flush(indexSegmentImpl, directory.getAbsolutePath());
	  
	  /*directory.mkdirs();
		File forwardIndexStorage = new File(directory, INDEX_FILE_NAME);
		RandomAccessFile forwardIndexFile = new RandomAccessFile(forwardIndexStorage, "rw");
		PropertiesConfiguration propertiesConfiguration = getPropertiesMetadata(directory);
		for (String column : indexSegmentImpl.getColumnTypes().keySet()) {
			ForwardIndex forwardIndex =  indexSegmentImpl.getForwardIndex(column);
			forwardIndex.getColumnMetadata().save(propertiesConfiguration);
			File dictionaryFile = getDictionaryFile(directory, column);
			OutputStream outputStream = new FileOutputStream(dictionaryFile);
			DictionaryPersistentManager.persist(outputStream, forwardIndex.getDictionary());
			outputStream.close();
			if (forwardIndex instanceof ForwardIndexImpl) {
			    ForwardIndexImpl forwardIndexImpl = (ForwardIndexImpl) forwardIndex;
			    forwardIndexImpl.getCompressedIntArray().getStorage().rewind();
			    forwardIndexFile.getChannel().write(forwardIndexImpl.getCompressedIntArray().getStorage(), forwardIndex.getColumnMetadata().getStartOffset());
	            forwardIndexFile.getChannel().force(true);
			} else if (forwardIndex instanceof SortedForwardIndexImpl){
			    String sortedIndexFileName = forwardIndex.getColumnMetadata().getColumn() + ".ranges";
                SortedIndexPersistentManager.persist(new FileOutputStream(new File(directory, sortedIndexFileName)), (SortedForwardIndexImpl) forwardIndex);
			}
		}
		propertiesConfiguration.save();
		forwardIndexFile.getFD().sync();
		forwardIndexFile.getChannel().close();
		forwardIndexFile.close();*/
	}
	public static GazelleIndexSegmentImpl read(File directory, boolean memoryMappedMode) throws Exception {
		return SegmentReader.read(directory, ReadMode.DBBuffer);
	  
	  /*RandomAccessFile forwardIndexFile = null;
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
        			if (columnMetadata.isSorted()) {
        			    SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionary, new int[columnMetadata.getNumberOfDictionaryValues()], new int[columnMetadata.getNumberOfDictionaryValues()], columnMetadata.getNumberOfElements(), columnMetadata);
        			    String sortedIndexFileName = columnMetadata.getColumn() + ".ranges";
        			    SortedIndexPersistentManager.readMinMaxRanges(new FileInputStream(new File(directory, sortedIndexFileName)), sortedForwardIndexImpl);
        			    indexSegmentImpl.getForwardIndexes().put(column, sortedForwardIndexImpl);
        			} else {
            			ByteBuffer byteBuffer = null;
            			if (memoryMappedMode) {
            				forwardIndexFile.getChannel().map(MapMode.READ_ONLY, columnMetadata.getStartOffset(), columnMetadata.getByteLength());
            			} else {
            				byteBuffer =  ByteBuffer.allocateDirect((int)columnMetadata.getByteLength());
            				forwardIndexFile.getChannel().read(byteBuffer, columnMetadata.getStartOffset());
            			}
            			CompressedIntArray compressedIntArray = new CompressedIntArray(columnMetadata.getNumberOfElements(), CompressedIntArray.getNumOfBits(columnMetadata.getNumberOfDictionaryValues()), byteBuffer); 
            			indexSegmentImpl.getForwardIndexes().put(column, new ForwardIndexImpl(column, compressedIntArray, dictionary, columnMetadata));
            		}
        			indexSegmentImpl.setLength(columnMetadata.getNumberOfElements());
    		    } finally {
    		        IOUtils.closeQuietly(inputStream);
    		    }
    		}
    		return indexSegmentImpl;
		} catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
		} finally {
    		if (!memoryMappedMode && forwardIndexFile != null) {
    			forwardIndexFile.close();
    		}
		}*/
		
	}
    public static PropertiesConfiguration getPropertiesMetadata(File directory) throws ConfigurationException {
        PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(new File(directory, METADATA_PROPERTIES));
        return propertiesConfiguration;
    }
	private static File getDictionaryFile(File directory, String column) {
		return new File(directory, column + ".dict");
	}
}
