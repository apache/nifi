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

package org.apache.nifi.processors.kite;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.spi.AbstractDatasetReader;
import org.kitesdk.data.spi.DataModelUtil;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.ReaderWriterState;

/**
 * This is a temporary addition. The version in 0.18.0 throws a NPE when the
 * InputStream constructor is used.
 */
class JSONFileReader<E> extends AbstractDatasetReader<E> {

  private final GenericData model;
  private final Schema schema;

  private InputStream incoming = null;

  // state
  private ReaderWriterState state = ReaderWriterState.NEW;
  private Iterator<E> iterator;

  public JSONFileReader(InputStream incoming, Schema schema, Class<E> type) {
    this.incoming = incoming;
    this.schema = schema;
    this.model = DataModelUtil.getDataModelForType(type);
    this.state = ReaderWriterState.NEW;
  }

  @Override
  public void initialize() {
    Preconditions.checkState(state.equals(ReaderWriterState.NEW),
        "A reader may not be opened more than once - current state:%s", state);
    Preconditions.checkArgument(Schema.Type.RECORD.equals(schema.getType()),
        "Schemas for JSON files should be record");

    this.iterator = Iterators.transform(JsonUtil.parser(incoming),
        new Function<JsonNode, E>() {
          @Override
          @SuppressWarnings("unchecked")
          public E apply(@Nullable JsonNode node) {
            return (E) JsonUtil.convertToAvro(model, node, schema);
          }
        });

    this.state = ReaderWriterState.OPEN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return iterator.hasNext();
  }

  @Override
  public E next() {
    Preconditions.checkState(state.equals(ReaderWriterState.OPEN),
        "Attempt to read from a file in state:%s", state);
    return iterator.next();
  }

  @Override
  public void close() {
    if (!state.equals(ReaderWriterState.OPEN)) {
      return;
    }

    iterator = null;
    try {
      incoming.close();
    } catch (IOException e) {
      throw new DatasetIOException("Unable to close reader path", e);
    }

    state = ReaderWriterState.CLOSED;
  }

  @Override
  public boolean isOpen() {
    return (this.state == ReaderWriterState.OPEN);
  }

}
