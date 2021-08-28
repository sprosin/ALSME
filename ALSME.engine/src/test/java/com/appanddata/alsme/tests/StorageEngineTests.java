package com.appanddata.alsme.tests;

import com.appanddata.alsme.engine.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StorageEngineTests {

    @Mock
    public IDataLog dataLog;
    @Mock
    public IMemtable memtable;
    @Mock
    public ISSTableProvider issTableProvider;

    @BeforeMethod
    public void init() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void simpleEnginePutTest() throws IOException {
        IStorageEngine engine = new StorageEngine(dataLog, memtable, issTableProvider);
        engine.put("key", "value");

        Mockito.verify(dataLog).append("key", new MemtableValue("value"));
        Mockito.verify(memtable).put("key", new MemtableValue("value"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void putNullTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.put("key", null);
        engine.clearData();
    }

    @Test
    public void simpleEnginePutGetTest() throws IOException {
        IMemtable memtable = new Memtable();
        IStorageEngine engine = new StorageEngine(dataLog, memtable, issTableProvider);
        engine.put("key", "value");

        String result = engine.get("key");
        assertThat(result, equalTo("value"));
    }

    @Test
    @Ignore
    public void add1GbDataTest() throws IOException {
        IMemtable memtable = new Memtable();
        ISSTableProvider provider = new SSTableProvider();
        IDataLog dataLog = new DataLog();
        IStorageEngine engine = new StorageEngine(dataLog, memtable, provider);
        engine.setMemoryDataLimit(10000000);
        engine.clearData();

        StringBuilder sb = new StringBuilder(10000);
        for (int j = 0; j < 100000; j++) {
            sb.append("a");
        }
        for (int i = 0; i < 10000; i++) {
            String number = Integer.toString(i);
            String s = sb.replace(0, number.length(), number).toString();
            engine.put("key" + i, s);
        }

        engine.clearData();

    }

    @Test
    public void memtableFlushTest() throws IOException {
        IMemtable memtable = new Memtable();
        ISSTableProvider provider = new SSTableProvider();
        IStorageEngine engine = new StorageEngine(dataLog, memtable, provider);
        engine.setMemoryDataLimit(100000);
        engine.clearData();

        StringBuilder sb = new StringBuilder(100000);
        for (int j = 0; j < 100000; j++) {
            sb.append("a");
        }

        engine.put("key", sb.toString());
        assertThat(memtable.getDataSize(), equalTo(0));

        engine.clearData();
    }

    @Test
    public void ssTableTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.setMemoryDataLimit(10);
        engine.clearData();
        engine.put("key", "valuevaluevalue");
        assertThat(memtable.getDataSize(), equalTo(0));

        String value = engine.get("key");
        assertThat(value, equalTo("valuevaluevalue"));

        engine.clearData();
    }

    @Test
    public void restoreMemtableSimpleTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.clearData();

        engine.put("key", "value");

        IStorageEngine newEngine = new StorageEngine();
        newEngine.recoverState();

        String newValue = newEngine.get("key");

        assertThat(newValue, equalTo("value"));
    }

    @Test
    public void multipleSSTableTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.clearData();
        engine.setMemoryDataLimit(10);

        engine.put("key", "valuevaluevalue");
        engine.put("key", "valuevaluevalue2");


        String newValue = engine.get("key");

        assertThat(newValue, equalTo("valuevaluevalue2"));
    }

    @Test
    public void removeSimpleTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.clearData();

        engine.put("key", "value");
        engine.remove("key");
        String newValue = engine.get("key");

        assertThat(newValue, nullValue());
    }

    @Test
    public void removeSSTableTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.clearData();
        engine.setMemoryDataLimit(50);

        engine.put("key", "valuevaluevaluevaluevaluevaluevaluevaluevalue"); // length 3+45+1
        engine.remove("key");
        engine.put("key2", "valuevaluevaluevaluevaluevaluevaluevaluevaluevalue"); // length 3+45+1
        String newValue = engine.get("key");

        assertThat(newValue, nullValue());
    }

    @Test
    public void removeSSTable2ndTest() throws IOException {
        IStorageEngine engine = new StorageEngine();
        engine.clearData();
        engine.setMemoryDataLimit(50);

        engine.put("key", "valuevaluevaluevaluevaluevaluevaluevaluevalue"); // length 3+45+1
        engine.remove("key");
        engine.put("key2", "valuevaluevaluevaluevaluevaluevaluevaluevaluevalue"); // length 3+45+1
        engine.put("key2", "valuevaluevaluevaluevaluevaluevaluevaluevaluevaluevalue"); // length 3+50+1
        String newValue = engine.get("key");

        assertThat(newValue, nullValue());
    }
}
