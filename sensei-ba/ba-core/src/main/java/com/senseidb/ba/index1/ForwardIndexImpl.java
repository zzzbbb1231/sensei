package com.senseidb.ba.index1;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.util.CompressedIntArray;

public class ForwardIndexImpl implements ForwardIndex {
	private CompressedIntArray compressedIntArray;
	private final String column;
	private TermValueList<?> dictionary;
	private ColumnMetadata columnMetadata;
	public ForwardIndexImpl(String column, CompressedIntArray compressedIntArray, TermValueList<?> dictionary, ColumnMetadata columnMetadata) {
		this.column = column;
		this.compressedIntArray = compressedIntArray;
		this.dictionary = dictionary;
		this.columnMetadata = columnMetadata;
	}
	@Override
	public int getLength() {
		return compressedIntArray.getCapacity();
	}

	@Override
	public int getValueIndex(int docId) {
		return compressedIntArray.readInt(docId);
	}

	@Override
	public int getFrequency(int valueId) {
		return 0;
	}

	@Override
	public TermValueList<?> getDictionary() {
		return dictionary;
	}

	

	public String getColumn() {
		return column;
	}

	public void setDictionary(TermValueList<?> dictionary) {
		this.dictionary = dictionary;
	}
	public ColumnMetadata getColumnMetadata() {
		return columnMetadata;
	}
	public CompressedIntArray getCompressedIntArray() {
		return compressedIntArray;
	}

}
