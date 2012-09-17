package com.senseidb.ba.index1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnType;
import com.senseidb.indexing.DefaultSenseiInterpreter;

public class DictionaryPersistentManager {
    public static void persist(OutputStream outputStream, TermValueList dictionary) throws IOException {
        DataOutputStream dataOutputStream = null;
        dataOutputStream = new DataOutputStream(outputStream);
    	try {
    	if (dictionary instanceof TermStringList) {
    		TermStringList termStringList = (TermStringList) dictionary;
    		for (int i = 0; i < termStringList.size(); i++) {
    			String str = termStringList.get(i);
    			byte[] bytes = str.getBytes("UTF8");
    			dataOutputStream.writeShort(bytes.length);
    			dataOutputStream.write(bytes);
    		}
    	} else if (dictionary instanceof TermIntList) {
    		TermIntList termIntList = (TermIntList) dictionary;
    		for (int i = 0; i < termIntList.size(); i++) {
    			dataOutputStream.writeInt(termIntList.getPrimitiveValue(i));
    		}
    	} else if (dictionary instanceof TermLongList) {
    		TermLongList termLongList = (TermLongList) dictionary;
    		for (int i = 0; i < termLongList.size(); i++) {
    			dataOutputStream.writeLong(termLongList.getPrimitiveValue(i));
    		}
    	} else if (dictionary instanceof TermFloatList) {
    		TermFloatList termFloatList = (TermFloatList) dictionary;
    		for (int i = 0; i < termFloatList.size(); i++) {
    			dataOutputStream.writeFloat(termFloatList.getPrimitiveValue(i));
    		}
    	} else {
    		throw new UnsupportedOperationException(dictionary.toString());
    	}
    	dataOutputStream.flush();
    	} finally {
    	    if (dataOutputStream != null) {
    	        dataOutputStream.close();
    	    }
    	}
    }
    public static TermValueList read(InputStream inputStream, int dictionarySize, ColumnType columnType) throws Exception {
    	DataInputStream dataInputStream = new DataInputStream(inputStream);
    	TermValueList ret = null;
    	if (columnType == ColumnType.STRING) {
    		TermStringList termStringList = new TermStringList();
    		for (int i = 0; i < dictionarySize; i++) {
    			int length = dataInputStream.readShort();
    			byte[] bytes = new byte[length];
    			dataInputStream.read(bytes);
    			String str = new String(bytes, "UTF8");
    			if (i != 0) {
    				termStringList.add(str);
    			} else {
    				termStringList.add(null);
    			}
    			ret = termStringList;
    			ret.seal();
    		}
    	}
    	if (columnType == ColumnType.INT) {
    		TermIntList termIntList = new TermIntList(DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(int.class));
    		int[] arr = new int[dictionarySize];
    		for (int i = 0; i < dictionarySize; i++) {
    			arr[i] = dataInputStream.readInt();
    		}
    		Field field = TermIntList.class.getDeclaredField("_elements");
    		field.setAccessible(true);
    		field.set(termIntList, arr);
    		ret = termIntList;
    		
    	} else if (columnType == ColumnType.LONG) {
    		TermLongList termLongList = new TermLongList(DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(long.class));
    		long[] arr = new long[dictionarySize];
    		for (int i = 0; i < dictionarySize; i++) {
    			arr[i] = dataInputStream.readLong();
    		}
    		Field field = TermLongList.class.getDeclaredField("_elements");
    		field.setAccessible(true);
    		field.set(termLongList, arr);
    		ret = termLongList;
    		
    	} else if (columnType == ColumnType.FLOAT) {
    		TermFloatList termFloatList = new TermFloatList(DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(float.class));
    		float[] arr = new float[dictionarySize];
    		for (int i = 0; i < dictionarySize; i++) {
    			arr[i] = dataInputStream.readFloat();
    		}
    		Field field = TermFloatList.class.getDeclaredField("_elements");
    		field.setAccessible(true);
    		field.set(termFloatList, arr);
    		ret = termFloatList;
    		
    	} else {
    		throw new UnsupportedOperationException(columnType.toString());
    	}
    	return ret;
    }
}
