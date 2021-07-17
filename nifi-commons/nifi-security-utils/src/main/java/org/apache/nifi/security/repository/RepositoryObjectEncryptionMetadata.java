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
package org.apache.nifi.security.repository;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.commons.codec.binary.Hex;

public abstract class RepositoryObjectEncryptionMetadata implements Serializable {
    public String keyId;
    public String algorithm;
    public byte[] ivBytes;
    public String version;
    public int cipherByteLength;
    private transient int length = 0;

    @Override
    public String toString() {
        String sb = "Repository Object Encryption Metadata" +
                " Key ID: " +
                keyId +
                " Algorithm: " +
                algorithm +
                " IV: " +
                Hex.encodeHexString(ivBytes) +
                " Version: " +
                version +
                " Cipher text length: " +
                cipherByteLength +
                " Serialized byte length: " +
                length();
        return sb;
    }

    public int length() {
        if (length == 0) {
            try {
                final ByteArrayOutputStream temp = new ByteArrayOutputStream(512);
                ObjectOutputStream oos = new ObjectOutputStream(temp);
                oos.writeObject(this);
                length = temp.size();
            } catch (IOException e) {
                throw new AssertionError("This is unreachable code");
            }
        }
        return length;
    }
}
