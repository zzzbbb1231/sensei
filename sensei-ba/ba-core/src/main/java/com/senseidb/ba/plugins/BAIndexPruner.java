package com.senseidb.ba.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.search.req.SenseiRequest;

public class BAIndexPruner implements SenseiIndexPruner {

    @Override
    public IndexReaderSelector getReaderSelector(SenseiRequest req) {
        final List<String> fields = new ArrayList<String>();
        fields.addAll(req.getSelectionNames());
        if (req.getSort() != null)
        for (SortField sortField : req.getSort()) {
            fields.add(sortField.getField());
        }
        fields.addAll(req.getFacetSpecs().keySet());
        return new IndexReaderSelector() {
            @Override
            public boolean isSelected(BoboIndexReader reader) throws IOException {
                IndexReader innerReader = reader.getInnerReader();
                if (innerReader instanceof SegmentToZoieReaderAdapter) {
                    return ((SegmentToZoieReaderAdapter) innerReader).getOfflineSegment().getColumnTypes().keySet().containsAll(fields);
                }
                return false;
            }
        };
    }

}
