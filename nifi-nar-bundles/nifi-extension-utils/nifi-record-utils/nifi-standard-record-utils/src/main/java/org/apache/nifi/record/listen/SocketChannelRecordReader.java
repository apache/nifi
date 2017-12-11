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
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Encapsulates a SocketChannel and a RecordReader for the channel.
 */
public interface SocketChannelRecordReader extends Closeable {

    /**
     * Currently a RecordReader can only be created with a FlowFile. Since we won't have a FlowFile at the time
     * a connection is accepted, this method will be used to lazily create the RecordReader later. Eventually this
     * method should be removed and the reader should be passed in through the constructor.
     *
     *
     * @param flowFile the flow file we are creating the reader for
     * @param logger the logger of the component creating the reader
     * @return a RecordReader
     *
     * @throws IllegalStateException if create is called after a reader has already been created
     */
    RecordReader createRecordReader(final FlowFile flowFile, final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException;

    /**
     * @return the RecordReader created by calling createRecordReader, or null if one has not been created yet
     */
    RecordReader getRecordReader();

    /**
     * @return the remote address of the underlying channel
     */
    InetAddress getRemoteAddress();

    /**
     * @return true if the underlying channel is closed, false otherwise
     */
    boolean isClosed();

}
