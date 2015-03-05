/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.kitesdk.data.spi.filesystem;

import au.com.bytecode.opencsv.CSVReader;
import java.io.InputStream;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.DescriptorUtil;
import org.kitesdk.data.spi.EntityAccessor;
import org.kitesdk.data.spi.ReaderWriterState;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

import static org.kitesdk.data.spi.filesystem.FileSystemProperties.REUSE_RECORDS;

/**
 * This is a temporary addition. The version in 0.18.0 throws a NPE when the
 * InputStream constructor is used.
 */
public class CSVFileReaderFixed<E> extends AbstractDatasetReader<E> {

  private final CSVProperties props;
  private final Schema schema;
  private final boolean reuseRecords;

  private final Class<E> recordClass;

  private CSVReader reader = null;
  private CSVRecordBuilder<E> builder;

  private InputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private boolean hasNext = false;
  private String[] next = null;
  private E record = null;

  public CSVFileReaderFixed(InputStream incoming, CSVProperties props,
                       Schema schema, Class<E> type) {
    this.incoming = incoming;
    this.schema = schema;
    this.recordClass = type;
    this.state = ReaderWriterState.NEW;
    this.props = props;
    this.reuseRecords = false;

    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for CSV files must be records of primitive types");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    this.reader = CSVUtil.newReader(incoming, props);

    List<String> header = null;
    if (props.useHeader) {
      this.hasNext = advance();
      header = Lists.newArrayList(next);
    } else if (props.header != null) {
      try {
        header = Lists.newArrayList(
            CSVUtil.newParser(props).parseLine(props.header));
      } catch (IOException e) {
        throw new DatasetIOException(
            "Failed to parse header from properties: " + props.header, e);
      }
    }

    this.builder = new CSVRecordBuilder<E>(schema, recordClass, header);

    // initialize by reading the first record
    this.hasNext = advance();

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return hasNext;
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);

    if (!hasNext) {
      throw new NoSuchElementException();
    }

    try {
      if (reuseRecords) {
        this.record = builder.makeRecord(next, record);
        return record;
      } else {
        return builder.makeRecord(next, null);
      }
    } finally {
      this.hasNext = advance();
    }
  }

  private boolean advance() {
    try {
      next = reader.readNext();
    } catch (IOException e) {
      throw new DatasetIOException("Could not read record", e);
    }
    return (next != null);
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    try {
      reader.close();
    } catch (IOException e) {
      throw new DatasetIOException("Unable to close reader", e);
    }

    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }
}
