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

/**
 * Exception when a chunk is malformed.
 * Chunks are independent within the file so we should be able to safely continue processing the remaining chunks.
 */
public class MalformedChunkException extends Exception {
    private final long offset;
    private final int chunkNum;
    private byte[] badChunk;

    public MalformedChunkException(String message, Throwable cause, long offset, int chunkNum, byte[] badChunk) {
        super(message, cause);
        this.offset = offset;
        this.chunkNum = chunkNum;
        this.badChunk = badChunk;
    }

    public byte[] getBadChunk() {
        return badChunk;
    }

    public int getChunkNum() {
        return chunkNum;
    }
}
