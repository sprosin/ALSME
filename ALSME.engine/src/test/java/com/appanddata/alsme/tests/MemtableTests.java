package com.appanddata.alsme.tests;

import com.appanddata.alsme.engine.IMemtable;
import com.appanddata.alsme.engine.Memtable;
import com.appanddata.alsme.engine.MemtableValue;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class MemtableTests {

    @Test
    public void simpleCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, new MemtableValue(value));
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, equalTo(key.length() + value.length() + 1));
    }

    @Test
    public void updateValueCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        MemtableValue mv1 = new MemtableValue("value");
        MemtableValue mv2 = new MemtableValue("value2");
        memtable.put(key, mv1);
        memtable.put(key, mv2);
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, equalTo(key.length() + mv2.length()));
    }

    @Test
    public void removeKeyCapacityTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, new MemtableValue(value));
        memtable.remove(key);
        int dataSize = memtable.getDataSize();
        assertThat(dataSize, greaterThan(0));
    }

    @Test
    public void removeMissingKeyTest() {
        IMemtable memtable = new Memtable();
        String key = "key";
        String value = "value";
        memtable.put(key, new MemtableValue(value));
        memtable.remove(key);
        memtable.remove(key);
    }


}
