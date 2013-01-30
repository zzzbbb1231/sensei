package com.senseidb.ba.gazelle.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public class GazelleIndexSegmentImpl implements IndexSegment {
	private Map<String, ColumnMetadata> columnMetatdaMap = new HashMap<String, ColumnMetadata>();
	private Map<String, TermValueList> termValueListMap = new HashMap<String, TermValueList>();
	private Map<String, ForwardIndex> forwardIndexMap = new HashMap<String, ForwardIndex>();
	private Map<String, DocIdSet[]> invertedIndexMap = new HashMap<String, DocIdSet[]>();
	private int length;
	private Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
	private SegmentMetadata segmentMetadata;

	@SuppressWarnings("rawtypes")
	public GazelleIndexSegmentImpl(ForwardIndex[] forwardIndexArr, TermValueList[] termValueListArr, ColumnMetadata[] columnMetadataArr, SegmentMetadata segmentMetadata, int length) {
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
	public GazelleIndexSegmentImpl(Map<String, ColumnMetadata> metadataMap, Map<String, ForwardIndex> forwardIndexMap, Map<String, TermValueList> termValueListMap, SegmentMetadata segmentMetadata, int length) {
		this.forwardIndexMap = forwardIndexMap;
		this.columnMetatdaMap = metadataMap;
		this.termValueListMap = termValueListMap;
		this.segmentMetadata = segmentMetadata;
		this.length = length;
		init();
	}
	public GazelleIndexSegmentImpl() {
		segmentMetadata = new SegmentMetadata();
	}
	private void init() {
		columnTypes = new HashMap<String, ColumnType>();
		for (String columnName : columnMetatdaMap.keySet()) {
			columnTypes.put(columnName, columnMetatdaMap.get(columnName).getColumnType());      
		}    
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
	public void initInvertedIndex(String columns) throws IOException{
		if(columns != null && columns != ""){
			String[] list = columns.split(",");
			for(String column : list){
				//Fetch all values that this column could take
				TermValueList values = termValueListMap.get(column);
				ForwardIndex fIndex = forwardIndexMap.get(column);
				
				if(values == null || fIndex == null){
					return;
				}
				
				//Somehow deal with MultiValueForwardIndexImpl1 instances...
				if(fIndex instanceof MultiValueForwardIndexImpl1){
					MultiValueForwardIndexImpl1 mIndex = (MultiValueForwardIndexImpl1) fIndex;
					MultiFacetIterator mIt = mIndex.getIterator();
					continue;
				}
								
				//Create correct number of inverted indices for this column
				int size = values.size();
				DocIdSet[] iIndices = new DocIdSet[size];
				for(int i = 1; i < size; i++){
					String value = values.get(i);
					iIndices[i] = new GazelleInvertedIndexImpl(fIndex, fIndex.getDictionary().indexOf(value));
				}
				
				size = fIndex.getLength();
				SingleValueForwardIndex temp = (SingleValueForwardIndex) fIndex;
				SingleValueRandomReader reader = temp.getReader();
				
				//Insert DocID into the correct index.
				for(int i = 0; i < size; i++){
					((GazelleInvertedIndexImpl) iIndices[reader.getValueIndex(i)]).addDoc(i);
				}
				invertedIndexMap.put(column, iIndices);
			}			
		}
	}

	@Override
	public DocIdSet[] getInvertedIndex(String column) {
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

}
