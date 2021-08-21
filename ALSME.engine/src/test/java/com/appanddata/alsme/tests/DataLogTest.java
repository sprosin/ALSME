package com.appanddata.alsme.tests;

import com.appanddata.alsme.engine.DataLog;
import com.appanddata.alsme.engine.IDataLog;
import org.hamcrest.MatcherAssert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class DataLogTest {

    IDataLog datalog;
    String filename = "test.log";

    @BeforeMethod
    public void setUpTest() {
        datalog = new DataLog(filename);
        datalog.clear();
    }

    @AfterMethod
    public void tearDownTest() {
        datalog.clear();
    }

    @Test
    public void testProcessLogFile() throws IOException {
        datalog.append("key", "valuevaluevalue");

        AtomicInteger processedItemsCount = new AtomicInteger();
        AtomicReference<String> returnedKey = new AtomicReference<>();
        AtomicReference<String> returnedValue = new AtomicReference<>();

        datalog.processLogFile((String key, String value) -> {
            processedItemsCount.getAndIncrement();
            returnedKey.set(key);
            returnedValue.set(value);
        });

        MatcherAssert.assertThat((processedItemsCount.get()), equalTo(1));
        MatcherAssert.assertThat(returnedKey.get(), equalTo("key"));
        MatcherAssert.assertThat(returnedValue.get(), equalTo("valuevaluevalue"));
    }

    @Test
    public void testProcessCorruptedLogFile() throws IOException {
        datalog.clear();
        datalog.append("key", "valuevaluevalue");

        corruptDataLogFile(filename);

        AtomicInteger processedItemsCount = new AtomicInteger();

        datalog.processLogFile((String key, String value) -> {
            processedItemsCount.getAndIncrement();
        });

        MatcherAssert.assertThat(processedItemsCount.get(), equalTo(0));
    }

    @Test
    public void testProcessCorruptedLogFileWithData() throws IOException {
        datalog.clear();
        datalog.append("key", "valuevaluevalue");
        datalog.append("key2", "valuevaluevalue2");

        corruptDataLogFile(filename);

        AtomicInteger processedItemsCount = new AtomicInteger();
        AtomicReference<String> returnedKey = new AtomicReference<>();
        AtomicReference<String> returnedValue = new AtomicReference<>();

        datalog.processLogFile((String key, String value) -> {
            processedItemsCount.getAndIncrement();
            returnedKey.set(key);
            returnedValue.set(value);
        });

        MatcherAssert.assertThat((processedItemsCount.get()), equalTo(1));
        MatcherAssert.assertThat(returnedKey.get(), equalTo("key"));
        MatcherAssert.assertThat(returnedValue.get(), equalTo("valuevaluevalue"));
    }

    private void corruptDataLogFile(String filename) {
        File file = new File(filename);

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(file.length() - 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}