package com.senseidb.ba.index1;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class IndexMetadata {
	private Map<String, ColumnMetadata> metadata = new TreeMap<String, ColumnMetadata>();

	public static IndexMetadata readFromConfiguration(PropertiesConfiguration config) throws ConfigurationException {
	    config.load();
		IndexMetadata ret = new IndexMetadata();
		Iterator keys = config.getKeys("column");
		while (keys.hasNext()) {
			String key = (String) keys.next();
			key = key.substring("column.".length());
			String property = key.substring(0, key.indexOf("."));
            ret.metadata.put(property, ColumnMetadata.readFromConfiguration(property,
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
