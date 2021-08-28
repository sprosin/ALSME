package com.appanddata.alsme.engine;

import java.util.Map;
import java.util.TreeMap;

public class Memtable implements IMemtable {
    TreeMap<String, MemtableValue> treeMap = new TreeMap<>();
    private int keysDataSize = 0;
    private int valuesDataSize = 0;


    @Override
    public void put(String key, MemtableValue value) {
        if (treeMap.containsKey(key)) {
            valuesDataSize -= treeMap.get(key).length();
        } else {
            keysDataSize += key.length();
        }
        treeMap.put(key, value);
        valuesDataSize += value.length();
    }

    @Override
    public String get(String key) {
        MemtableValue mv = treeMap.get(key);
        return (mv == null || mv.isTombstone()) ? null : mv.getValue();
    }

    @Override
    public int getDataSize() {
        return keysDataSize + valuesDataSize;
    }

    @Override
    public void remove(String key) {
        put(key, MemtableValue.getDeletedValue());
    }

    @Override
    public Iterable<? extends Map.Entry<String, MemtableValue>> entrySet() {
        return treeMap.entrySet();
    }

    @Override
    public void clear() {
        treeMap.clear();
        keysDataSize = 0;
        valuesDataSize = 0;
    }
}
