package com.senseidb.ba.gazelle.impl;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.MetadataAware;
import com.senseidb.ba.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public class MultiValueForwardIndexImpl1 implements MultiValueForwardIndex, MetadataAware {
    private final CompressedMultiArray compressedMultiArray;
    private final ColumnMetadata columnMetadata;
    private final TermValueList<?> dictionary;

    public MultiValueForwardIndexImpl1(String column, CompressedMultiArray compressedMultiArray, TermValueList<?> dictionary, ColumnMetadata columnMetadata) {
        this.compressedMultiArray = compressedMultiArray;
        this.dictionary = dictionary;
        this.columnMetadata = columnMetadata;
    }
    @Override
    public int getLength() {
        return columnMetadata.getNumberOfElements();
    }

    @Override
    public TermValueList<?> getDictionary() {
        return dictionary;
    }

    @Override
    public ColumnType getColumnType() {
        return columnMetadata.getColumnType();
    }

    @Override
    public MultiFacetIterator getIterator() {
        return compressedMultiArray.iterator();
    }

    @Override
    public int randomRead(int[] buffer, int index) {
        return compressedMultiArray.randomRead(buffer, index);
    }

    @Override
    public int getMaxNumValuesPerDoc() {
        return compressedMultiArray.getMaxNumValuesPerDoc();
    }
    public CompressedMultiArray getCompressedMultiArray() {
        return compressedMultiArray;
    }
    public ColumnMetadata getColumnMetadata() {
        return columnMetadata;
    }
    
}
