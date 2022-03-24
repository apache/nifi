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

package org.apache.nifi.processors.evtx.parser;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.evtx.parser.bxml.NameStringNode;
import org.apache.nifi.processors.evtx.parser.bxml.TemplateNode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A Chunk is a self-contained group of templates, strings, and nodes
 */
public class ChunkHeader extends Block {
    public static final String ELF_CHNK = "ElfChnk";
    private final String magicString;
    private final UnsignedLong fileFirstRecordNumber;
    private final UnsignedLong fileLastRecordNumber;
    private final UnsignedLong logFirstRecordNumber;
    private final UnsignedLong logLastRecordNumber;
    private final UnsignedInteger headerSize;
    private final UnsignedInteger lastRecordOffset;
    private final int nextRecordOffset;
    private final UnsignedInteger dataChecksum;
    private final String unused;
    private final UnsignedInteger headerChecksum;
    private final Map<Integer, NameStringNode> nameStrings;
    private final Map<Integer, TemplateNode> templateNodes;
    private final int chunkNumber;
    private final ComponentLog log;
    private UnsignedLong recordNumber;

    public ChunkHeader(BinaryReader binaryReader, ComponentLog log, long headerOffset, int chunkNumber) throws IOException {
        super(binaryReader, headerOffset);
        this.log = log;
        this.chunkNumber = chunkNumber;
        CRC32 crc32 = new CRC32();
        crc32.update(binaryReader.peekBytes(120));

        magicString = binaryReader.readString(8);
        fileFirstRecordNumber = binaryReader.readQWord();
        fileLastRecordNumber = binaryReader.readQWord();
        logFirstRecordNumber = binaryReader.readQWord();
        logLastRecordNumber = binaryReader.readQWord();
        headerSize = binaryReader.readDWord();
        lastRecordOffset = binaryReader.readDWord();
        nextRecordOffset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid next record offset.");
        dataChecksum = binaryReader.readDWord();
        unused = binaryReader.readString(68);

        if (!ELF_CHNK.equals(magicString)) {
            throw new IOException("Invalid magic string " + this);
        }

        headerChecksum = binaryReader.readDWord();

        // These are included into the checksum
        crc32.update(binaryReader.peekBytes(384));

        if (crc32.getValue() != headerChecksum.longValue()) {
            throw new IOException("Invalid checksum " + this);
        }
        if (lastRecordOffset.compareTo(UnsignedInteger.valueOf(Integer.MAX_VALUE)) > 0) {
            throw new IOException("Last record offset too big to fit into signed integer");
        }

        nameStrings = new HashMap<>();
        for (int i = 0; i < 64; i++) {
            int offset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid offset.");
            while (offset > 0) {
                NameStringNode nameStringNode = new NameStringNode(new BinaryReader(binaryReader, offset), this);
                nameStrings.put(offset, nameStringNode);
                offset = NumberUtil.intValueMax(nameStringNode.getNextOffset(), Integer.MAX_VALUE, "Invalid offset.");
            }
        }

        templateNodes = new HashMap<>();
        for (int i = 0; i < 32; i++) {
            int offset = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid offset.");
            while (offset > 0) {
                int token = new BinaryReader(binaryReader, offset - 10).read();
                if (token != 0x0c) {
                    log.warn("Unexpected token when parsing template at offset " + offset);
                    break;
                }
                BinaryReader templateReader = new BinaryReader(binaryReader, offset - 4);
                int pointer = NumberUtil.intValueMax(templateReader.readDWord(), Integer.MAX_VALUE, "Invalid pointer.");
                if (offset != pointer) {
                    log.warn("Invalid pointer when parsing template at offset " + offset);
                    break;
                }
                TemplateNode templateNode = new TemplateNode(templateReader, this);
                templateNodes.put(offset, templateNode);
                offset = templateNode.getNextOffset();
            }
        }
        crc32 = new CRC32();
        crc32.update(binaryReader.peekBytes(nextRecordOffset - 512));
        if (crc32.getValue() != dataChecksum.longValue()) {
            throw new IOException("Invalid data checksum " + this);
        }
        recordNumber = fileFirstRecordNumber.minus(UnsignedLong.ONE);
    }

    public NameStringNode addNameStringNode(int offset, BinaryReader binaryReader) throws IOException {
        NameStringNode nameStringNode = new NameStringNode(binaryReader, this);
        nameStrings.put(offset, nameStringNode);
        return nameStringNode;
    }

    public TemplateNode addTemplateNode(int offset, BinaryReader binaryReader) throws IOException {
        TemplateNode templateNode = new TemplateNode(binaryReader, this);
        templateNodes.put(offset, templateNode);
        return templateNode;
    }

    public TemplateNode getTemplateNode(int offset) {
        return templateNodes.get(offset);
    }

    @Override
    public String toString() {
        return "ChunkHeader{" +
                "magicString='" + magicString + '\'' +
                ", fileFirstRecordNumber=" + fileFirstRecordNumber +
                ", fileLastRecordNumber=" + fileLastRecordNumber +
                ", logFirstRecordNumber=" + logFirstRecordNumber +
                ", logLastRecordNumber=" + logLastRecordNumber +
                ", headerSize=" + headerSize +
                ", lastRecordOffset=" + lastRecordOffset +
                ", nextRecordOffset=" + nextRecordOffset +
                ", dataChecksum=" + dataChecksum +
                ", unused='" + unused + '\'' +
                ", headerChecksum=" + headerChecksum +
                '}';
    }

    public boolean hasNext() {
        return logLastRecordNumber.compareTo(recordNumber) > 0;
    }

    public String getString(int offset) {
        NameStringNode nameStringNode = nameStrings.get(offset);
        if (nameStringNode == null) {
            return null;
        }
        return nameStringNode.getString();
    }

    @VisibleForTesting
    Map<Integer, NameStringNode> getNameStrings() {
        return Collections.unmodifiableMap(nameStrings);
    }

    @VisibleForTesting
    Map<Integer, TemplateNode> getTemplateNodes() {
        return Collections.unmodifiableMap(templateNodes);
    }

    public int getChunkNumber() {
        return chunkNumber;
    }

    public Record next() throws IOException {
        if (!hasNext()) {
            return null;
        }
        try {
            Record record = new Record(getBinaryReader(), this);
            recordNumber = record.getRecordNum();
            return record;
        } catch (IOException e) {
            recordNumber = fileLastRecordNumber;
            throw e;
        }
    }
}
