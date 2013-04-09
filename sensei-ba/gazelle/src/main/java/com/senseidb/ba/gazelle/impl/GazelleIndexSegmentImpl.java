package com.senseidb.ba.gazelle.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.custom.GazelleCustomIndex;
import com.senseidb.ba.gazelle.impl.HighCardinalityInvertedIndex.GazelleInvertedHighCardinalitySet;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class GazelleIndexSegmentImpl implements IndexSegment {
	private Map<String, ColumnMetadata> columnMetatdaMap = new HashMap<String, ColumnMetadata>();
	private Map<String, TermValueList> termValueListMap = new HashMap<String, TermValueList>();
	private Map<String, ForwardIndex> forwardIndexMap = new HashMap<String, ForwardIndex>();
	private Map<String, InvertedIndex> invertedIndexMap = new HashMap<String, InvertedIndex>();
	private Map<String, GazelleCustomIndex> customIndexes = new HashMap<String, GazelleCustomIndex>();
	private int length;
	private String associatedDirectory;
	private Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
	private SegmentMetadata segmentMetadata;
	private String[] invertedColumns;
	
	private Boolean highCardinality = false;

	public static Timer invertedIndicesCreationTime = Metrics.newTimer(new MetricName(GazelleIndexSegmentImpl.class ,"invertedIndicesCreationTime"), TimeUnit.MILLISECONDS, TimeUnit.DAYS);

	@SuppressWarnings("rawtypes")
	public GazelleIndexSegmentImpl(ForwardIndex[] forwardIndexArr, TermValueList[] termValueListArr, ColumnMetadata[] columnMetadataArr, SegmentMetadata segmentMetadata, int length) throws IOException {
		this.length = length;
		for (int i = 0; i < forwardIndexArr.length; i++) {
			forwardIndexMap.put(columnMetadataArr[i].getName(), forwardIndexArr[i]);
			termValueListMap.put(columnMetadataArr[i].getName(), termValueListArr[i]);
			columnMetatdaMap.put(columnMetadataArr[i].getName(), columnMetadataArr[i]);
		}
		init();
		this.segmentMetadata = segmentMetadata;
	}
	@SuppressWarnings("rawtypes")
	public GazelleIndexSegmentImpl(ForwardIndex[] forwardIndexArr, TermValueList[] termValueListArr, ColumnMetadata[] columnMetadataArr, SegmentMetadata segmentMetadata, int length, String[] invertedColumns) throws IOException {
		this.length = length;
		for (int i = 0; i < forwardIndexArr.length; i++) {
			forwardIndexMap.put(columnMetadataArr[i].getName(), forwardIndexArr[i]);
			termValueListMap.put(columnMetadataArr[i].getName(), termValueListArr[i]);
			columnMetatdaMap.put(columnMetadataArr[i].getName(), columnMetadataArr[i]);
		}
		this.invertedColumns = invertedColumns;
		init();
		this.segmentMetadata = segmentMetadata;
	}
	
	@SuppressWarnings("rawtypes")
	public GazelleIndexSegmentImpl(Map<String, ColumnMetadata> metadataMap, Map<String, ForwardIndex> forwardIndexMap, Map<String, TermValueList> termValueListMap, SegmentMetadata segmentMetadata, int length, String[] invertedColumns) throws IOException {
		this.forwardIndexMap = forwardIndexMap;
		this.columnMetatdaMap = metadataMap;
		this.termValueListMap = termValueListMap;
		this.segmentMetadata = segmentMetadata;
		this.length = length;
		this.invertedColumns = invertedColumns;
		init();
	}
	public GazelleIndexSegmentImpl() {
		segmentMetadata = new SegmentMetadata();
	}
	private void init() throws IOException {
		columnTypes = new HashMap<String, ColumnType>();
		for (String columnName : columnMetatdaMap.keySet()) {
			columnTypes.put(columnName, columnMetatdaMap.get(columnName).getColumnType());      
		}
		initInvertedIndex(invertedColumns);
	}


	public Map<String, ColumnMetadata> getColumnMetadataMap() {
		return columnMetatdaMap;
	}
	public Map<String, TermValueList> getDictionaries() {
		return termValueListMap;
	}
	public Map<String, ForwardIndex> getForwardIndexes() {
		
	  return forwardIndexMap;
	}
	@Override
	public Map<String, ColumnType> getColumnTypes() {		
	  return columnTypes;
	}

	@Override
	public TermValueList<?> getDictionary(String column) {		
	  return termValueListMap.get(column);
	}

	/**
	 * This method initializes inverted indices (We don't use a vanilla implementation of inverted indices, read GazelleInvertedIndex class for more info.
	 * @param columns --> Specifies the column on which we want to build inverted indices on
	 * @throws IOException --> Comes from addDoc
	 */
	public void initInvertedIndex(String[] columns) throws IOException{
		if(columns != null){
			long elapsedTime = System.currentTimeMillis();
			for(String column : columns){
				//Fetch all values that this column could take
				TermValueList values = termValueListMap.get(column);
				ForwardIndex forwardIndex = forwardIndexMap.get(column);
				
				int maxMultiVals = -1;

				if (forwardIndex instanceof SortedForwardIndexImpl || forwardIndex instanceof SecondarySortedForwardIndexImpl || values == null || forwardIndex == null){
					continue;
				}

				//Create correct number of inverted indices for this column
				int size = values.size();
				
				int ratio = forwardIndex.getLength()/size;
				
				if(ratio > 100000){
					continue;
				}
				
				if(forwardIndex instanceof MultiValueForwardIndexImpl1){
					MultiValueForwardIndexImpl1 multiIndex = (MultiValueForwardIndexImpl1) forwardIndex;
					maxMultiVals = multiIndex.getMaxNumValuesPerDoc();
				}

				//Create the normal GazelleInvertedIndexImpl if the size of the dictionary isn't too large
				if(ratio > 100 || maxMultiVals > 100){
					InvertedIndex invertedIndices;
					//We estimate the jump value for one dictionary value and assume it will work for the others (Otherwise, we waste too much time
					//on estimation of the jump value.
					int optimalValue = StandardCardinalityInvertedIndex.estimateOptimalMinJump(forwardIndex, forwardIndex.getDictionary().indexOf(values.get(1)));
					invertedIndices = new StandardCardinalityInvertedIndex(forwardIndex, size, maxMultiVals > -1 ? 0 : optimalValue, values);

					//If it's a MultiValueForwardIndex, we have to deal with it differently.
					if(forwardIndex instanceof MultiValueForwardIndexImpl1){
						MultiValueForwardIndexImpl1 multiIndex = (MultiValueForwardIndexImpl1) forwardIndex;
						int[] buffer = new int[multiIndex.getMaxNumValuesPerDoc()];

						for(int i = 0; i < length; i++) {
							int count = multiIndex.randomRead(buffer, i);
							for (int j = 0; j < count; j++) {
								int valueId = buffer[j];
								((StandardCardinalityInvertedIndex) invertedIndices).addDoc(i, valueId);
							}
						}

					}
					else{
						size = forwardIndex.getLength();
						SingleValueForwardIndex temp = (SingleValueForwardIndex) forwardIndex;
						SingleValueRandomReader reader = temp.getReader();

						//Insert DocID into the correct index.
						for(int i = 0; i < size; i++){
							if(!((StandardCardinalityInvertedIndex) invertedIndices).invertedIndexPresent(reader.getValueIndex(i))){
								continue;
							}
							((StandardCardinalityInvertedIndex) invertedIndices).addDoc(i, reader.getValueIndex(i));
						}
					}
					((StandardCardinalityInvertedIndex) invertedIndices).flush();
					((StandardCardinalityInvertedIndex) invertedIndices).optimize();

					invertedIndexMap.put(column, invertedIndices);
				}
				
				//If the size of the dictionary is too large, we create a specialized GazelleInvertedIndexHighCardinalityImpl
				else{
					highCardinality = true;
					
					HighCardinalityInvertedIndex invertedIndices = new HighCardinalityInvertedIndex(forwardIndex, size);
					
					//Prepare the data for use.
					invertedIndices.prepData();

					invertedIndexMap.put(column, invertedIndices);
				}
			}
			invertedIndicesCreationTime.update(System.currentTimeMillis() - elapsedTime, TimeUnit.MILLISECONDS); 
		}
	}

	@Override
	public InvertedIndex getInvertedIndex(String column) {
		return invertedIndexMap.get(column);
	}

	@Override
	public ForwardIndex getForwardIndex(String column) {
		return forwardIndexMap.get(column);
	}

	public SegmentMetadata getSegmentMetadata() {
		return segmentMetadata;
	}

	public void setSegmentMetadata(SegmentMetadata segmentMetadata) {
		this.segmentMetadata = segmentMetadata;
	}

	@Override
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public long getInvertedDocCount() {
		if(highCardinality == false){
			return StandardCardinalityInvertedIndex.getTotalCount();
		}
		else{
			return HighCardinalityInvertedIndex.getTotalCount();
		}
	}
	public long getInvertedCompressionRate() {
		if(highCardinality == false){
			return StandardCardinalityInvertedIndex.getTotalCompSize();
		}
		else{
			return HighCardinalityInvertedIndex.getTotalCompSize();
		}
	}
	public long getTotalInvertedDocCount() {
		if(highCardinality == false){
			return StandardCardinalityInvertedIndex.getTotalTrueCount();
		}
		else{
			return HighCardinalityInvertedIndex.getTotalTrueCount();
		}
	}
  public String getAssociatedDirectory() {
    return associatedDirectory;
  }
  public void setAssociatedDirectory(String associatedDirectory) {
    this.associatedDirectory = associatedDirectory;
  }
  public Map<String, GazelleCustomIndex> getCustomIndexes() {
    return customIndexes;
  }
  public void setCustomIndexes(Map<String, GazelleCustomIndex> customIndexes) {
    this.customIndexes = customIndexes;
  }
  public void addCustomIndex(GazelleCustomIndex gazelleCustomIndex, String column) {
    getCustomIndexes().put(column, gazelleCustomIndex);
  }
  public void addCustomIndex(GazelleCustomIndex gazelleCustomIndex, Map<String, ColumnMetadata> properties) {
    getColumnMetadataMap().putAll(properties);
    for (String column : properties.keySet()) {
      getCustomIndexes().put(column, gazelleCustomIndex);
      getColumnTypes().put(column, gazelleCustomIndex.getColumnType(column));
      getForwardIndexes().put(column, gazelleCustomIndex.getForwardIndex(column));
      getDictionaries().put(column, gazelleCustomIndex.getDictionary(column));
      InvertedIndex invertedIndex = gazelleCustomIndex.getInvertedIndex(column);
      if (invertedIndex != null) {
        invertedIndexMap.put(column, invertedIndex);
      }
      
    }
    
  }
 
}
