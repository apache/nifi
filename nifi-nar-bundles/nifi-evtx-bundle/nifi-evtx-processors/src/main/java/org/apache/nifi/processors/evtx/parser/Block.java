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

import java.io.IOException;

/**
 * Parent class of all Blocks and BxmlNodes.  Tracks offset, initialPosition.  Has common initialization logic.
 */
public abstract class Block {
    private final long offset;
    private final int initialPosition;
    private BinaryReader binaryReader;
    private boolean initialized = false;

    public Block(BinaryReader binaryReader) {
        this(binaryReader, binaryReader.getPosition());
    }

    public Block(BinaryReader binaryReader, long offset) {
        this.binaryReader = binaryReader;
        this.initialPosition = binaryReader.getPosition();
        this.offset = offset;
    }

    public BinaryReader getBinaryReader() {
        return binaryReader;
    }

    public long getOffset() {
        return offset;
    }

    protected void init(boolean clearBinaryReader) throws IOException {
        if (initialized) {
            throw new IOException("Initialize should only be called once");
        } else {
            initialized = true;
        }
        int skipAmount = getHeaderLength() - (binaryReader.getPosition() - initialPosition);
        if (skipAmount > 0) {
            binaryReader.skip(skipAmount);
        }

        if (clearBinaryReader) {
            clearBinaryReader();
        }
    }

    protected void init() throws IOException {
        init(true);
    }

    protected void clearBinaryReader() {
        this.binaryReader = null;
    }

    protected int getHeaderLength() {
        return 0;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public int getInitialPosition() {
        return initialPosition;
    }
}
