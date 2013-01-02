package com.senseidb.search.req.mapred.impl.dictionary;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;

public class FloatDictionaryAccessor implements DictionaryNumberAccessor {
    private final TermFloatList dictionary;

    public FloatDictionaryAccessor(TermFloatList dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public int getIntValue(int valueId) {
        return (int) dictionary.getPrimitiveValue(valueId);
    }

    @Override
    public float getFloatValue(int valueId) {
        return dictionary.getPrimitiveValue(valueId);
    }

    @Override
    public long getLongValue(int valueId) {
        return (long) dictionary.getPrimitiveValue(valueId);
    }

    @Override
    public double getDoubleValue(int valueId) {
        return dictionary.getPrimitiveValue(valueId);
    }

    }