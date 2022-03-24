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
package org.apache.nifi.remote.util;

import java.io.InputStream;
import java.util.Map;

import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.MinimumLengthInputStream;

public class StandardDataPacket implements DataPacket {

    private final Map<String, String> attributes;
    private final InputStream stream;
    private final long size;

    public StandardDataPacket(final Map<String, String> attributes, final InputStream stream, final long size) {
        this.attributes = attributes;
        this.stream = new MinimumLengthInputStream(new LimitingInputStream(stream, size), size);
        this.size = size;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public InputStream getData() {
        return stream;
    }

    public long getSize() {
        return size;
    }

}
