package com.senseidb.search.filter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.EmptyFilter;
import com.senseidb.util.Util;

public class AscendingTimeFilter extends Filter {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private final String      _columnName;
  private final TimeUnit    _timeUnit;
  private final long        _normalizedTime;

  public AscendingTimeFilter(String columnName, TimeUnit timeUnit, long fromTime) {
    _columnName = columnName;
    _timeUnit = timeUnit;
    _normalizedTime = _timeUnit.convert(fromTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    Util.ensureBoboReader(reader);
    BoboIndexReader boboReader = (BoboIndexReader) reader;
    Object data = boboReader.getFacetData(_columnName);
    if (data == null || !(data instanceof FacetDataCache)) {
      throw new IllegalStateException("facet is not defined on column: "
          + _columnName);
    }
    FacetDataCache dataCache = (FacetDataCache) data;
    
    int idx = dataCache.valArray.indexOf(_normalizedTime);
    if (idx < 0) {
      idx = -(idx + 1);
      if (idx == dataCache.valArray.size())
        return EmptyFilter.getInstance().getDocIdSet(reader);
    }

    String termString = dataCache.valArray.get(idx);

    TermDocs td = reader.termDocs(new Term(_columnName, termString));
    try {
      final int maxDoc = dataCache.orderArray.size();
      if (td != null) {
        if (td.next()) {
          final int doc = td.doc();
          if (doc != DocIdSetIterator.NO_MORE_DOCS) {
            return new DocIdSet() {

              @Override
              public DocIdSetIterator iterator() throws IOException {
                return new DocIdSetIterator() {

                  int docid = doc-1;

                  @Override
                  public int nextDoc() throws IOException {
                    if (docid < maxDoc) {
                      docid++;
                    } else {
                      docid = DocIdSetIterator.NO_MORE_DOCS;
                    }

                    return docid;
                  }

                  @Override
                  public int docID() {
                    return docid;
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    if (docid < target) {
                      docid = target;
                    }
                    return docid;
                  }
                };
              }

            };
          }
        }
      }

      return EmptyFilter.getInstance().getDocIdSet(reader);
    } 
    finally {
      if (td != null) {
        td.close();
      }
    }

  }

}
