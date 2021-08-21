package com.appanddata.alsme.engine;

import java.util.Map;
import java.util.TreeMap;

public class Memtable implements IMemtable {
    TreeMap<String, String> treeMap = new TreeMap<String, String>();
    private int keysDataSize = 0;
    private int valuesDataSize = 0;


    @Override
    public void put(String key, String value) {
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
        return treeMap.get(key);
    }

    @Override
    public int getDataSize() {
        return keysDataSize + valuesDataSize;
    }

    @Override
    public void remove(String key) {
        if (treeMap.containsKey(key)) {
            keysDataSize -= key.length();
            valuesDataSize -= treeMap.get(key).length();
            treeMap.remove(key);
        } else
            throw new IllegalArgumentException("Key %s doesn't exist".formatted(key));
    }

    @Override
    public Iterable<? extends Map.Entry<String, String>> entrySet() {
        return treeMap.entrySet();
    }

    @Override
    public void clear() {
        treeMap.clear();
        keysDataSize = 0;
        valuesDataSize = 0;
    }
}
