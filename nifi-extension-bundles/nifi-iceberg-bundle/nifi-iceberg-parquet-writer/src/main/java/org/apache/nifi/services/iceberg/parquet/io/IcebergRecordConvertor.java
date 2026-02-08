package org.apache.nifi.services.iceberg.parquet.io;

import org.apache.iceberg.data.Record;


public interface IcebergRecordConvertor {
    void convertRecord(Record record);
}
