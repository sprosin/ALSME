package com.appanddata.alsme.tests;

import com.appanddata.alsme.engine.IMemtable;
import com.appanddata.alsme.engine.Memtable;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class MemtableTests {

    @Test
    public void simpleCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, value);
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, equalTo(key.length() + value.length()));
    }

    @Test
    public void updateValueCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        String value2 = "value2";
        memtable.put(key, value);
        memtable.put(key, value2);
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, equalTo(key.length() + value2.length()));
    }

    @Test
    public void removeKeyCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, value);
        memtable.remove(key);
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, equalTo(0));
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void removeMissingKeyTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, value);
        memtable.remove(key);
        memtable.remove(key);
    }


}
