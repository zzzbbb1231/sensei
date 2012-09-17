package com.senseidb.ba.index1;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.Configuration;

public class IndexMetadata {
	private Map<String, ColumnMetadata> metadata = new TreeMap<String, ColumnMetadata>();

	public static IndexMetadata readFromConfiguration(Configuration config) {
		IndexMetadata ret = new IndexMetadata();
		Iterator keys = config.getKeys("column.");
		while (keys.hasNext()) {
			String key = (String) keys.next();
			key = key.substring("column.".length());
			ret.metadata.put(key.substring(0, key.indexOf(",")), null);
		}
		for (Map.Entry<String, ColumnMetadata> entry : ret.metadata.entrySet()) {
			entry.setValue(ColumnMetadata.readFromConfiguration(entry.getKey(),
					config));
		}
		return ret;
	}

	public void save(Configuration config) {
		for (ColumnMetadata columnMetadata : metadata.values()) {
			columnMetadata.save(config);
		}
	}

	public Map<String, ColumnMetadata> getMetadata() {
		return metadata;
	}

}
