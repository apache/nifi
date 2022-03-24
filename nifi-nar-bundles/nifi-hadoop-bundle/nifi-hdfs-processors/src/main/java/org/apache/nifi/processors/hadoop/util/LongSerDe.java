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
package org.apache.nifi.processors.hadoop.util;

import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LongSerDe implements Serializer<Long>, Deserializer<Long> {

    @Override
    public Long deserialize(final byte[] input) throws DeserializationException, IOException {
        if ( input == null || input.length == 0 ) {
            return null;
        }

        final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(input));
        return dis.readLong();
    }

    @Override
    public void serialize(final Long value, final OutputStream out) throws SerializationException, IOException {
        final DataOutputStream dos = new DataOutputStream(out);
        dos.writeLong(value);
    }

}
