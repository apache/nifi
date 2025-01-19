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

package org.apache.nifi.processors.evtx.parser.bxml.value;

import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.bxml.BxmlNode;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Node containing a system timestamp
 */
public class SystemtimeTypeNode extends VariantTypeNode {
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String value;

    public SystemtimeTypeNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent, int length) throws IOException {
        super(binaryReader, chunkHeader, parent, length);
        int year = binaryReader.readWord();
        int month = binaryReader.readWord();
        final int monthOfYear = month + 1;
        binaryReader.readWord(); // dayOfWeek
        int day = binaryReader.readWord();
        int hour = binaryReader.readWord();
        int minute = binaryReader.readWord();
        int second = binaryReader.readWord();
        int millisecond = binaryReader.readWord();
        final int nanosecond = millisecond * 1000000;
        final LocalDateTime localDateTime = LocalDateTime.of(year, monthOfYear, day, hour, minute, second, nanosecond);
        value = FORMATTER.format(localDateTime);
    }

    @Override
    public String getValue() {
        return value;
    }
}
