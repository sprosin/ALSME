package com.appanddata.alsme.engine;

@FunctionalInterface
public interface IKeyValueProcessing {
    void process(String key, String value, boolean tombstone);
}
