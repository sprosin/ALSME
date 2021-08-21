package com.appanddata.alsme.engine;

import java.io.IOException;

public interface IDataLog {
    void append(String key, String value) throws IOException;

    void clear();

    void processLogFile(IKeyValueProcessing keyValueProcessing) throws IOException;
}
