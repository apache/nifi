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
package org.apache.nifi.security.crypto.key.io;

import java.nio.ByteBuffer;

/**
 * Byte Buffer Search utilities
 */
public class ByteBufferSearch {

    private static final int END_OF_FILE = -1;

    /**
     * Get starting index of delimiter in buffer
     *
     * @param buffer Byte Buffer to be searched
     * @param delimiter Delimiter
     * @return Starting index of delimiter or -1 when not found
     */
    public static int indexOf(final ByteBuffer buffer, final byte[] delimiter) {
        final int bufferSearchLength = buffer.limit() - delimiter.length + 1;
        bufferSearch:
        for (int i = 0; i < bufferSearchLength; i++) {
            for (int j = 0; j < delimiter.length; j++) {
                final int bufferIndex = i + j;
                final byte indexByte = buffer.get(bufferIndex);
                final byte delimiterByte = delimiter[j];
                if (indexByte != delimiterByte) {
                    continue bufferSearch;
                }
            }
            return i;
        }
        return END_OF_FILE;
    }
}
