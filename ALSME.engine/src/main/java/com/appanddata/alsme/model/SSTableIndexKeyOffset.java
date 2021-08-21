package com.appanddata.alsme.model;

public class SSTableIndexKeyOffset {
    private final String key;
    private final int offset;

    public SSTableIndexKeyOffset(String key, int offset) {
        this.key = key;
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public int getOffset() {
        return offset;
    }
}
