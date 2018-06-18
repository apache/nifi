/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.streaming;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class HiveRecordWriter extends AbstractRecordWriter {

    private RecordReader recordReader;
    private NiFiRecordSerDe serde;
    private ComponentLog log;

    public HiveRecordWriter(RecordReader recordReader, ComponentLog log) {
        super(null);
        this.recordReader = recordReader;
        this.log = log;
    }

    @Override
    public AbstractSerDe createSerde() throws SerializationError {
        try {
            Properties tableProps = table.getMetadata();
            tableProps.setProperty(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(inputColumns));
            tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(":").join(inputTypes));
            NiFiRecordSerDe serde = new NiFiRecordSerDe(recordReader, log);
            SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
            this.serde = serde;
            return serde;
        } catch (SerDeException e) {
            throw new SerializationError("Error initializing serde " + NiFiRecordSerDe.class.getName(), e);
        }
    }

    @Override
    public Object encode(byte[] bytes) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support encoding of records via bytes, only via an InputStream");
    }

    @Override
    public void write(long writeId, byte[] record) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support writing of records via bytes, only via an InputStream");
    }

    @Override
    public void write(long writeId, InputStream inputStream) throws StreamingException {
        // The inputStream is already available to the recordReader, so just iterate through the records
        try {
            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                write(writeId, record);
            }
        } catch (MalformedRecordException | IOException e) {
            throw new StreamingException(e.getLocalizedMessage(), e);
        }
    }

    public Object encode(Record record) throws SerializationError {
        try {
            ObjectWritable blob = new ObjectWritable(record);
            return serde.deserialize(blob);
        } catch (SerDeException e) {
            throw new SerializationError("Unable to convert Record into Object", e);
        }
    }

    private void write(long writeId, Record record) throws StreamingException {
        checkAutoFlush();
        try {
            Object encodedRow = encode(record);
            int bucket = getBucket(encodedRow);
            List<String> partitionValues = getPartitionValues(encodedRow);
            getRecordUpdater(partitionValues, bucket).insert(writeId, encodedRow);
            conn.getConnectionStats().incrementRecordsWritten();
        } catch (IOException e) {
            throw new StreamingIOFailure("Error writing record in transaction write id (" + writeId + ")", e);
        }
    }
}
