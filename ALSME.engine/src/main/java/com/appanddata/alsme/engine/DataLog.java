package com.appanddata.alsme.engine;

import com.appanddata.alsme.model.avro.KeyValue;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class DataLog implements IDataLog {
    private String logFilename = "memtable.log";

    public DataLog(String logFilename) {
        this.logFilename = logFilename;
    }

    public DataLog() {
    }

    @Override
    public void append(String key, MemtableValue value) throws IOException {
        File logFile = new File(logFilename);

        DatumWriter<KeyValue> keyValueDatumWriter = new SpecificDatumWriter<>(KeyValue.class);
        try (DataFileWriter<KeyValue> dataFileWriter = new DataFileWriter<>(keyValueDatumWriter)) {

            if (!logFile.exists())
                dataFileWriter.create(KeyValue.getClassSchema(), logFile);
            else
                dataFileWriter.appendTo(logFile);

            KeyValue kv = new KeyValue(key, value.getValue(), value.isTombstone());
            dataFileWriter.append(kv);
        }
    }

    @Override
    public void clear() {
        File logFile = new File(logFilename);
        if (logFile.exists())
            logFile.delete();
    }

    @Override
    public void processLogFile(IKeyValueProcessing keyValueProcessing) throws IOException {
        File logFile = new File(logFilename);

        DatumReader<KeyValue> keyValueDatumReader = new SpecificDatumReader<>(KeyValue.class);
        try (DataFileReader<KeyValue> dataFileReader = new DataFileReader<>(logFile, keyValueDatumReader)) {
            KeyValue kv = null;
            while (dataFileReader.hasNext()) {
                kv = dataFileReader.next(kv);
                keyValueProcessing.process(kv.getKey().toString(), kv.getValue().toString(), kv.getTombstone());
            }
        }
    }


}
