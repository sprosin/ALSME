package com.appanddata.alsme.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SSTableIndex {
    Map<String, Integer> keyOffsets = new HashMap<String, Integer>();
    private final String filename;

    public SSTableIndex(String filename, Collection<SSTableIndexKeyOffset> keyOffsetCollection) {
        this.filename = filename;
        for (SSTableIndexKeyOffset keyOffset :
                keyOffsetCollection) {
            keyOffsets.put(keyOffset.getKey(), keyOffset.getOffset());
        }
    }


    public int size() {
        return keyOffsets.size();
    }

    public int get(String key) {
        return keyOffsets.get(key);
    }

    public boolean containsKey(String key) {
        return keyOffsets.containsKey(key);
    }

    public String getFilename() {
        return this.filename;
    }
}
