package com.senseidb.ba.index1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

public class SortedIndexPersistentManager {
    public static void persist(OutputStream outputStream, SortedForwardIndexImpl sortedForwardIndexImpl) throws IOException {
        DataOutputStream dataOutputStream = null;
        try {
            dataOutputStream = new DataOutputStream(outputStream);
            for (int i = 0; i < sortedForwardIndexImpl.getColumnMetadata().getNumberOfDictionaryValues(); i++) {
                dataOutputStream.writeInt(sortedForwardIndexImpl.getMinDocIds()[i]);
                dataOutputStream.writeInt(sortedForwardIndexImpl.getMaxDocIds()[i]);
            }
            dataOutputStream.flush();
        } finally {
            if (dataOutputStream != null) {
                dataOutputStream.close();
            }
        }
    }
    public static void readMinMaxRanges(InputStream inputStream, SortedForwardIndexImpl sortedForwardIndexImpl) throws IOException {
        DataInputStream dataInputStream= null;
        try {
            dataInputStream = new DataInputStream(inputStream);
            for (int i = 0; i < sortedForwardIndexImpl.getColumnMetadata().getNumberOfDictionaryValues(); i++) {
               sortedForwardIndexImpl.getMinDocIds()[i] = dataInputStream.readInt();
               sortedForwardIndexImpl.getMaxDocIds()[i] = dataInputStream.readInt();
            }
        } finally {
            IOUtils.closeQuietly(dataInputStream);
        }
    }
}
