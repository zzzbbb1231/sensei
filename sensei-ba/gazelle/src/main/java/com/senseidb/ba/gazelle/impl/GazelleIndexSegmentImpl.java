package com.senseidb.ba.gazelle.impl;

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
import com.senseidb.ba.gazelle.InvertedIndexObject;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.impl.GazelleInvertedIndexHighCardinalityImpl.GazelleInvertedHighCardinalitySet;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class GazelleIndexSegmentImpl implements IndexSegment {
	private Map<String, ColumnMetadata> columnMetatdaMap = new HashMap<String, ColumnMetadata>();
	private Map<String, TermValueList> termValueListMap = new HashMap<String, TermValueList>();
	private Map<String, ForwardIndex> forwardIndexMap = new HashMap<String, ForwardIndex>();
	private Map<String, InvertedIndexObject> invertedIndexMap = new HashMap<String, InvertedIndexObject>();
	private int length;
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
	public GazelleIndexSegmentImpl(Map<String, ColumnMetadata> metadataMap, Map<String, ForwardIndex> forwardIndexMap, Map<String, TermValueList> termValueListMap, SegmentMetadata segmentMetadata, int length) throws IOException {
		this.forwardIndexMap = forwardIndexMap;
		this.columnMetatdaMap = metadataMap;
		this.termValueListMap = termValueListMap;
		this.segmentMetadata = segmentMetadata;
		this.length = length;
		init();
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
				ForwardIndex fIndex = forwardIndexMap.get(column);

				if (fIndex instanceof SortedForwardIndexImpl || fIndex instanceof SecondarySortedForwardIndexImpl || values == null || fIndex == null){
					continue;
				}

				//Create correct number of inverted indices for this column
				int size = values.size();
				
				int ratio = fIndex.getLength()/size;
				
				if(ratio > 100000){
					continue;
				}

				//Create the normal GazelleInvertedIndexImpl if the size of the dictionary isn't too large
				if(ratio > 100){
					InvertedIndexObject iIndices;
					//We estimate the jump value for one dictionary value and assume it will work for the others (Otherwise, we waste too much time
					//on estimation of the jump value.
					int optVal = GazelleInvertedIndexImpl.estimateOptimalMinJump(fIndex, fIndex.getDictionary().indexOf(values.get(1)));
					iIndices = new GazelleInvertedIndexImpl(fIndex, size, optVal, values);

					//If it's a MultiValueForwardIndex, we have to deal with it differently.
					if(fIndex instanceof MultiValueForwardIndexImpl1){
						MultiValueForwardIndexImpl1 mIndex = (MultiValueForwardIndexImpl1) fIndex;
						int[] buffer = new int[mIndex.getMaxNumValuesPerDoc()];

						for(int i = 0; i < length; i++) {
							int count = mIndex.randomRead(buffer, i);
							for (int j = 0; j < count; j++) {
								int valueId = buffer[j];
								((GazelleInvertedIndexImpl) iIndices).addDoc(i, valueId);
							}
						}

					}
					else{
						size = fIndex.getLength();
						SingleValueForwardIndex temp = (SingleValueForwardIndex) fIndex;
						SingleValueRandomReader reader = temp.getReader();

						//Insert DocID into the correct index.
						for(int i = 0; i < size; i++){
							if(((GazelleInvertedIndexImpl) iIndices).checkNull(reader.getValueIndex(i)) == null){
								continue;
							}
							((GazelleInvertedIndexImpl) iIndices).addDoc(i, reader.getValueIndex(i));
						}
					}
					((GazelleInvertedIndexImpl) iIndices).flush();
					((GazelleInvertedIndexImpl) iIndices).optimize();

					invertedIndexMap.put(column, iIndices);
				}
				
				//If the size of the dictionary is too large, we create a specialized GazelleInvertedIndexHighCardinalityImpl
				else{
					highCardinality = true;
					
					GazelleInvertedIndexHighCardinalityImpl iIndices = new GazelleInvertedIndexHighCardinalityImpl(fIndex, size);
					
					//Prepare the data for use.
					iIndices.prepData();

					invertedIndexMap.put(column, iIndices);
				}
			}
			invertedIndicesCreationTime.update(System.currentTimeMillis() - elapsedTime, TimeUnit.MILLISECONDS); 
		}
	}

	@Override
	public InvertedIndexObject getInvertedIndex(String column) {
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
			return GazelleInvertedIndexImpl.getTotalCount();
		}
		else{
			return GazelleInvertedIndexHighCardinalityImpl.getTotalCount();
		}
	}
	public long getInvertedCompressionRate() {
		if(highCardinality == false){
			return GazelleInvertedIndexImpl.getTotalCompSize();
		}
		else{
			return GazelleInvertedIndexHighCardinalityImpl.getTotalCompSize();
		}
	}
	public long getTotalInvertedDocCount() {
		if(highCardinality == false){
			return GazelleInvertedIndexImpl.getTotalTrueCount();
		}
		else{
			return GazelleInvertedIndexHighCardinalityImpl.getTotalTrueCount();
		}
	}

}
