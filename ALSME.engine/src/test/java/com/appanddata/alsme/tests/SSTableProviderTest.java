package com.appanddata.alsme.tests;

import com.appanddata.alsme.engine.*;
import com.appanddata.alsme.model.SSTableIndex;
import org.hamcrest.MatcherAssert;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SSTableProviderTest {

    @Test
    public void saveMemtableToFileTest() throws IOException {
        ISSTableProvider issTableProvider = new SSTableProvider();
        IMemtable memtable = new Memtable();
        memtable.put("key", new MemtableValue("value"));
        issTableProvider.saveMemtable(memtable);

        File f = new File("alsme0.sstable");
        Assert.assertTrue(f.exists());
        issTableProvider.clearAllData();
    }

    @Test
    public void buildSSTableTest() throws IOException {
        ISSTableProvider issTableProvider = new SSTableProvider();
        issTableProvider.clearAllData();

        IMemtable memtable = new Memtable();
        memtable.put("key", new MemtableValue("value"));
        issTableProvider.saveMemtable(memtable);

        SSTableIndex[] ssTableIndices = issTableProvider.buildSSTables();
        MatcherAssert.assertThat(ssTableIndices.length, equalTo(1));

        MatcherAssert.assertThat(ssTableIndices[0].size(), equalTo(1));
        MatcherAssert.assertThat(ssTableIndices[0].get("key"), equalTo(0));

    }

    @Test
    public void returnNthValueTest() throws IOException {
        ISSTableProvider issTableProvider = new SSTableProvider();
        issTableProvider.clearAllData();

        IMemtable memtable = new Memtable();
        memtable.put("key", new MemtableValue("value"));
        memtable.put("key2", new MemtableValue("value2"));
        issTableProvider.saveMemtable(memtable);

        MemtableValue value = issTableProvider.getValue("alsme0.sstable", 1);

        MatcherAssert.assertThat(value, equalTo(new MemtableValue("value2", false)));

    }
}