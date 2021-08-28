package com.appanddata.alsme.engine;

import com.appanddata.alsme.model.SSTableIndex;

import java.io.IOException;

public interface ISSTableProvider {
    SSTableIndex buildSSTable(String filename) throws IOException;

    String saveMemtable(IMemtable memtable) throws IOException;

    void clearAllData();

    String generateNewFilename();

    SSTableIndex[] buildSSTables() throws IOException;

    MemtableValue getValue(String filename, int offset) throws IOException;
}
