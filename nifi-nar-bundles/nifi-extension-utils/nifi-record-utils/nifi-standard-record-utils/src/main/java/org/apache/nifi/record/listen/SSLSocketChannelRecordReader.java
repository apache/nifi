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
package org.apache.nifi.record.listen;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;
import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannelInputStream;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.channels.SocketChannel;

/**
 * Encapsulates an SSLSocketChannel and a RecordReader created for the given channel.
 */
public class SSLSocketChannelRecordReader implements SocketChannelRecordReader {

    private final SocketChannel socketChannel;
    private final SSLSocketChannel sslSocketChannel;
    private final RecordReaderFactory readerFactory;
    private final SocketChannelRecordReaderDispatcher dispatcher;

    private RecordReader recordReader;

    public SSLSocketChannelRecordReader(final SocketChannel socketChannel,
                                        final SSLSocketChannel sslSocketChannel,
                                        final RecordReaderFactory readerFactory,
                                        final SocketChannelRecordReaderDispatcher dispatcher) {
        this.socketChannel = socketChannel;
        this.sslSocketChannel = sslSocketChannel;
        this.readerFactory = readerFactory;
        this.dispatcher = dispatcher;
    }

    @Override
    public RecordReader createRecordReader(final FlowFile flowFile, final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        if (recordReader != null) {
            throw new IllegalStateException("Cannot create RecordReader because already created");
        }

        final InputStream in = new SSLSocketChannelInputStream(sslSocketChannel);
        recordReader = readerFactory.createRecordReader(flowFile, in, logger);
        return recordReader;
    }

    @Override
    public RecordReader getRecordReader() {
        return recordReader;
    }

    @Override
    public InetAddress getRemoteAddress() {
        return socketChannel.socket().getInetAddress();
    }

    @Override
    public boolean isClosed() {
        return sslSocketChannel.isClosed();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(recordReader);
        IOUtils.closeQuietly(sslSocketChannel);
        dispatcher.connectionCompleted();
    }

}
