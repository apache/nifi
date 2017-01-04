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
package org.apache.nifi.processors.lumberjack.frame;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encodes a LumberjackFrame into raw bytes using the given charset.
 */
@Deprecated
public class LumberjackEncoder {


    public byte[] encode(final LumberjackFrame frame) {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // Writes the version
        buffer.write(frame.getVersion());

        // Writes the frameType
        buffer.write(frame.getFrameType());

        // Writes the sequence number
        try {
            buffer.write(frame.getPayload());
        } catch (IOException e) {
            throw new LumberjackFrameException("Error decoding Lumberjack frame: " + e.getMessage(), e);
        }

        return buffer.toByteArray();
    }

}