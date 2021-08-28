package com.appanddata.alsme.engine;

import com.appanddata.alsme.model.SSTableIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StorageEngine implements IStorageEngine {

    List<SSTableIndex> ssTableIndexes = new ArrayList<>();
    private final IDataLog dataLog;
    private final ISSTableProvider issTableProvider;
    private int memoryDataLimit = 0;
    private final IMemtable memtable;

    public StorageEngine(IDataLog dataLog, IMemtable memtable, ISSTableProvider issTableProvider) {
        this.dataLog = dataLog;
        this.memtable = memtable;
        this.issTableProvider = issTableProvider;
    }

    public StorageEngine() {
        this(new DataLog(), new Memtable(), new SSTableProvider());
    }

    @Override
    public void put(String key, String value) throws IOException {
        if (value == null)
            throw new IllegalArgumentException("value cannot be null");

        MemtableValue mv = new MemtableValue(value);

        appendToLog(key, mv);
        addToMemtable(key, mv);
        if (memoryDataLimit > 0 && memtable.getDataSize() >= memoryDataLimit)
            flushMemtable();
    }

    private void flushMemtable() throws IOException {
        String newSSTableFilename = issTableProvider.saveMemtable(memtable);
        SSTableIndex ssTableIndex = issTableProvider.buildSSTable(newSSTableFilename);
        ssTableIndexes.add(0, ssTableIndex);
        memtable.clear();
        dataLog.clear();
    }

    private void addToMemtable(String key, MemtableValue value) {
        memtable.put(key, value);
    }

    private void appendToLog(String key, MemtableValue value) throws IOException {
        dataLog.append(key, value);
    }

    @Override
    public String get(String key) throws IOException {
        String value = memtable.get(key);
        if (value == null) {
            for (SSTableIndex ssTableIndex :
                    ssTableIndexes) {
                if (ssTableIndex.containsKey(key)) {
                    MemtableValue mv = issTableProvider.getValue(ssTableIndex.getFilename(), ssTableIndex.get(key));
                    return mv.isTombstone() ? null : mv.getValue();
                }
            }
        }
        return value;
    }

    @Override
    public void setMemoryDataLimit(int limit) {
        this.memoryDataLimit = limit;
    }

    @Override
    public void clearData() {
        dataLog.clear();
        memtable.clear();
        issTableProvider.clearAllData();
    }

    @Override
    public void recoverState() throws IOException {
        dataLog.processLogFile((String key, String value, boolean tombstone) -> {
            memtable.put(key, new MemtableValue(value, tombstone));
        });
        ssTableIndexes.clear();
        ssTableIndexes.addAll(Arrays.asList(issTableProvider.buildSSTables()));
    }

    @Override
    public void remove(String key) throws IOException {
        MemtableValue mv = MemtableValue.getDeletedValue();

        appendToLog(key, mv);
        addToMemtable(key, mv);
        if (memoryDataLimit > 0 && memtable.getDataSize() >= memoryDataLimit)
            flushMemtable();
    }
}
