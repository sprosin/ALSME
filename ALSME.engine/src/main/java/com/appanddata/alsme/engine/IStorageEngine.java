package com.appanddata.alsme.engine;

import java.io.IOException;

public interface IStorageEngine {
    void put(String key, String value) throws IOException;

    String get(String key) throws IOException;

    void setMemoryDataLimit(int limit);

    void clearData();

    void recoverState() throws IOException;
}
