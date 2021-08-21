package com.appanddata.alsme.engine;


import com.appanddata.alsme.model.SSTableIndex;
import com.appanddata.alsme.model.SSTableIndexKeyOffset;
import com.appanddata.alsme.model.avro.KeyValue;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SSTableProvider implements ISSTableProvider {

    private final String filenameFormat = "alsme%d.sstable";

    public String generateNewFilename() {
        int num = 0;
        String filenameext = filenameFormat.formatted(num);
        File file = new File(filenameext);
        while (file.exists()) {
            filenameext = filenameFormat.formatted(++num);
            file = new File(filenameext);
        }
        return file.getName();
    }

    @Override
    public SSTableIndex[] buildSSTables() throws IOException {
        List<SSTableIndex> ssTableIndices = new ArrayList<SSTableIndex>();

        int num = 0;
        String filenameext = filenameFormat.formatted(num);
        File file = new File(filenameext);
        while (file.exists()) {

            SSTableIndex ssTableIndex = buildSSTable(filenameext);
            ssTableIndices.add(ssTableIndex);

            filenameext = filenameFormat.formatted(++num);
            file = new File(filenameext);
        }

        Collections.reverse(ssTableIndices);


        return ssTableIndices.toArray(new SSTableIndex[0]);
    }

    @Override
    public String getValue(String filename, int offset) throws IOException {
        DatumReader<KeyValue> keyValueDatumReader = new SpecificDatumReader<KeyValue>(KeyValue.class);
        try (DataFileReader<KeyValue> dataFileReader = new DataFileReader<KeyValue>(new File(filename), keyValueDatumReader)) {
            int currentOffset = offset;
            long blockSize = dataFileReader.getBlockSize();
            if (blockSize > 0)
                while (currentOffset > blockSize) {
                    dataFileReader.nextBlock();
                    currentOffset -= blockSize;
                }

            KeyValue kv = null;
            while (currentOffset >= 0 && dataFileReader.hasNext()) {
                kv = dataFileReader.next(kv);
                currentOffset--;
            }

            if (currentOffset != -1)
                throw new IllegalStateException("requested record with offset %d not found in file %s".formatted(offset, filename));


            return kv.getValue().toString();
        }
    }

    @Override
    public SSTableIndex buildSSTable(String filename) throws IOException {
        List<SSTableIndexKeyOffset> keyOffsetList = new ArrayList<>();

        File logFile = new File(filename);

        int offset = 0;

        DatumReader<KeyValue> keyValueDatumReader = new SpecificDatumReader<KeyValue>(KeyValue.class);
        try (DataFileReader<KeyValue> dataFileReader = new DataFileReader<KeyValue>(logFile, keyValueDatumReader)) {
            KeyValue kv = null;
            while (dataFileReader.hasNext()) {
                kv = dataFileReader.next(kv);
                keyOffsetList.add(new SSTableIndexKeyOffset(kv.getKey().toString(), offset++));
            }
        }

        SSTableIndex newFileIndex = new SSTableIndex(filename, keyOffsetList);

        return newFileIndex;
    }

    @Override
    public String saveMemtable(IMemtable memtable) throws IOException {
        String newFilename = generateNewFilename();

        DatumWriter<KeyValue> keyValueDatumWriter = new SpecificDatumWriter<KeyValue>(KeyValue.class);
        try (DataFileWriter<KeyValue> dataFileWriter = new DataFileWriter<KeyValue>(keyValueDatumWriter)) {

            dataFileWriter.create(KeyValue.getClassSchema(), new File(newFilename));

            for (Map.Entry<String, String> keyValueEntry :
                    memtable.entrySet()) {
                KeyValue kv = new KeyValue(keyValueEntry.getKey(), keyValueEntry.getValue());
                dataFileWriter.append(kv);
            }
        }

        return newFilename;
    }

    @Override
    public void clearAllData() {
        int num = 0;
        String filenameext = filenameFormat.formatted(num);
        File file = new File(filenameext);
        while (file.exists()) {
            file.delete();
            filenameext = filenameFormat.formatted(++num);
            file = new File(filenameext);
        }
    }
}
