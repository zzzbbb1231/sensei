package com.senseidb.ba.index1;

import org.apache.commons.configuration.Configuration;

import com.senseidb.ba.ColumnType;

public class ColumnMetadata {
	private String column;
	private long startOffset;
	private long byteLength;
	private int numberOfElements;
	private int numberOfDictionaryValues;
	private int bitsPerElement;
	private ColumnType columnType;
	private boolean sorted;
	public static ColumnMetadata readFromConfiguration(String column, Configuration configuration) {
		ColumnMetadata ret = new ColumnMetadata();
		ret.column = column;
		ret.startOffset = configuration.getLong("column." + column + ".startOffset");
		ret.byteLength= configuration.getLong("column." + column + ".byteLength");
		ret.numberOfElements= configuration.getInt("column." + column + ".numberOfElements");
		ret.numberOfDictionaryValues= configuration.getInt("column." + column + ".numberOfDictionaryValues");
		ret.bitsPerElement= configuration.getInt("column." + column + ".bitsPerElement");
		ret.columnType= ColumnType.valueOfStr(configuration.getString("column." + column + ".columnType"));
		ret.sorted= configuration.getBoolean("column." + column + ".sorted", false);
		return ret;
	}
	
	public void save(Configuration configuration) {
		configuration.setProperty("column." + column + ".startOffset", startOffset);
		configuration.setProperty("column." + column + ".numberOfElements", numberOfElements);
		configuration.setProperty("column." + column + ".byteLength",byteLength);
		configuration.setProperty("column." + column + ".numberOfDictionaryValues", numberOfDictionaryValues);
		configuration.setProperty("column." + column + ".bitsPerElement", bitsPerElement);
		configuration.setProperty("column." + column + ".sorted", sorted);
		configuration.setProperty("column." + column + ".columnType", columnType.toString());
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public long getStartOffset() {
		return startOffset;
	}
	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}
	public long getByteLength() {
		return byteLength;
	}
	public void setByteLength(long byteLength) {
		this.byteLength = byteLength;
	}
	public int getNumberOfElements() {
		return numberOfElements;
	}
	public void setNumberOfElements(int numberOfElements) {
		this.numberOfElements = numberOfElements;
	}
	public int getNumberOfDictionaryValues() {
		return numberOfDictionaryValues;
	}
	public void setNumberOfDictionaryValues(int numberOfDictionaryValues) {
		this.numberOfDictionaryValues = numberOfDictionaryValues;
	}
	public int getBitsPerElement() {
		return bitsPerElement;
	}
	public void setBitsPerElement(int bitsPerElement) {
		this.bitsPerElement = bitsPerElement;
	}
	public boolean isSorted() {
		return sorted;
	}
	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}
	public ColumnType getColumnType() {
		return columnType;
	}
	public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    } 
}
