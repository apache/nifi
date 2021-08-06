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
package org.apache.nifi.distributed.cache.client.adapter;

import io.netty.buffer.ByteBuf;
import org.apache.nifi.distributed.cache.client.Deserializer;

import java.io.IOException;

/**
 * Tracker for receipt of incoming tokens from an InputStream.
 *
 * @param <T> the expected type of the service response token
 */
public class InboundToken<T> {

    /**
     * Flag indicating that all bytes needed to deserialize a token have been received.
     */
    private boolean isComplete;

    /**
     * The length of the byte stream expected from the service response.
     */
    private Integer length;

    /**
     * The received service method response value.  Depending on the data type and the deserializer
     * implementation, it is possible to deserialize a null value from a stream.
     */
    private T value;

    /**
     * Constructor.
     */
    public InboundToken() {
        reset();
    }

    /**
     * Initialize this object to receive bytes from a stream to be deserialized.
     */
    public void reset() {
        isComplete = false;
        length = null;
        value = null;
    }

    /**
     * Update the state of the received bytes to be deserialized.
     *
     * @param byteBuf      the intermediate buffer used to cache bytes received from the service
     * @param deserializer the deserializer to be used to construct the service response object
     * @throws IOException on serialization failures
     */
    public void update(final ByteBuf byteBuf, final Deserializer<T> deserializer) throws IOException {
        if ((length == null) && (byteBuf.readableBytes() >= Integer.BYTES)) {
            length = byteBuf.readInt();
        }
        if ((length != null) && (byteBuf.readableBytes() >= length)) {
            final byte[] bytes = new byte[length];
            byteBuf.readBytes(bytes);
            value = deserializer.deserialize(bytes);
            isComplete = true;
        }
    }

    /**
     * @return true, iff the service response byte stream for the token has been completely received
     */
    public boolean isComplete() {
        return isComplete;
    }

    /**
     * @return the service method response value
     */
    public T getValue() {
        return value;
    }
}
