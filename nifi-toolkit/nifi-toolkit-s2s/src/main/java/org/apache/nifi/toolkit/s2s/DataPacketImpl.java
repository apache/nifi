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

package org.apache.nifi.toolkit.s2s;

import org.apache.nifi.remote.protocol.DataPacket;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements DataPacket either taking the data passed in or reading from a file on disk
 */
public class DataPacketImpl implements DataPacket {
    private final Map<String, String> attributes;
    private final byte[] data;
    private final String dataFile;

    public DataPacketImpl(Map<String, String> attributes, byte[] data, String dataFile) {
        this.attributes = attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(attributes));
        this.data = data;
        this.dataFile = dataFile;
    }

    @Override
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @Override
    public InputStream getData() {
        if (data == null) {
            if (dataFile != null && dataFile.length() > 0) {
                try {
                    return new FileInputStream(dataFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return new ByteArrayInputStream(new byte[0]);
            }
        }
        return new ByteArrayInputStream(data);
    }

    @Override
    public long getSize() {
        if (data == null) {
            if (dataFile != null && dataFile.length() > 0) {
                return new File(dataFile).length();
            } else {
                return 0;
            }
        }
        return data.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataPacketImpl that = (DataPacketImpl) o;

        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
        return Arrays.equals(data, that.data);

    }

    @Override
    public int hashCode() {
        int result = attributes != null ? attributes.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
