package com.appanddata.alsme.engine;

import java.util.Map;

public interface IMemtable {
    void put(String key, MemtableValue value);

    String get(String key);

    int getDataSize();

    void remove(String key);

    Iterable<? extends Map.Entry<String, MemtableValue>> entrySet();

    void clear();
}
