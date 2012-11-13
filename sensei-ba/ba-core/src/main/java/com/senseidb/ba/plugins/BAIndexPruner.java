package com.senseidb.ba.plugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.SortField;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.indexing.SenseiIndexPruner;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.search.req.SenseiRequest;

public class BAIndexPruner implements SenseiIndexPruner, SenseiPlugin {

    private SenseiPluginRegistry pluginRegistry;
    private ArrayList<String> customFacetNames;

    @Override
    public IndexReaderSelector getReaderSelector(SenseiRequest req) {
        final List<String> fields = new ArrayList<String>();
        fields.addAll(req.getSelectionNames());
        if (req.getSort() != null)
        for (SortField sortField : req.getSort()) {
            fields.add(sortField.getField());
        }
        fields.addAll(req.getFacetSpecs().keySet());
        fields.removeAll(customFacetNames);
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

    @Override
    public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
      this.pluginRegistry = pluginRegistry;
      
    }

    @Override
    public void start() {
      List<FacetHandler> customFacetHandlers = pluginRegistry.resolveBeansByListKey(SenseiPluginRegistry.FACET_CONF_PREFIX, FacetHandler.class);
      customFacetNames = new ArrayList<String>();
      for (FacetHandler facetHandler : customFacetHandlers) {
        customFacetNames.add(facetHandler.getName());
      }
    }

    @Override
    public void stop() {
      
    }

    @Override
    public void sort(List<BoboIndexReader> readers) {
      
    }

}
