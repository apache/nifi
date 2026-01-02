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
package org.apache.nifi.services.iceberg.parquet.io;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.apache.nifi.services.iceberg.IcebergRowWriter;

import java.io.IOException;
import java.util.Objects;

/**
 * Standard implementation of Iceberg Row Writer wrapping an Iceberg Task Writer for abstracted access to iceberg-io modules
 */
public class ParquetIcebergRowWriter implements IcebergRowWriter {
    private final TaskWriter<Record> writer;

    public ParquetIcebergRowWriter(final TaskWriter<Record> writer) {
        this.writer = Objects.requireNonNull(writer, "Writer required");
    }

    @Override
    public void write(final Record row) throws IOException {
        writer.write(row);
    }

    @Override
    public void abort() throws IOException {
        writer.abort();
    }

    @Override
    public DataFile[] dataFiles() throws IOException {
        return writer.dataFiles();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
