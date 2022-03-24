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
package org.apache.nifi.event.transport.netty.channel;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;

public class RecordReaderSession implements Closeable {
    private final RecordReaderFactory readerFactory;
    private SocketAddress senderAddress;
    private RecordReader recordReader;
    private final InputStream recordStream;
    private final ComponentLog logger;

    public RecordReaderSession(final SocketAddress sender, final InputStream recordStream, final RecordReaderFactory factory, final ComponentLog logger) {
        this.senderAddress = sender;
        this.readerFactory = factory;
        this.logger = logger;
        this.recordStream = recordStream;
    }

    public SocketAddress getSender() {
        return senderAddress;
    }

    public RecordReader getRecordReader() throws SchemaNotFoundException, MalformedRecordException, IOException {
        if (recordReader == null) {
            recordReader = readerFactory.createRecordReader(null, recordStream, logger);
        }
        return recordReader;
    }

    @Override
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }
}
