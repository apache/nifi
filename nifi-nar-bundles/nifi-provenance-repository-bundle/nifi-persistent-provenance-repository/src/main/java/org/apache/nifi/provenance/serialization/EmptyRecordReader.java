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

package org.apache.nifi.provenance.serialization;

import java.io.IOException;

import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.toc.TocReader;

public class EmptyRecordReader implements RecordReader {

    @Override
    public void close() throws IOException {
    }

    @Override
    public StandardProvenanceEventRecord nextRecord() throws IOException {
        return null;
    }

    @Override
    public void skip(long bytesToSkip) throws IOException {
    }

    @Override
    public void skipTo(long position) throws IOException {
    }

    @Override
    public void skipToBlock(int blockIndex) throws IOException {
    }

    @Override
    public int getBlockIndex() {
        return 0;
    }

    @Override
    public boolean isBlockIndexAvailable() {
        return false;
    }

    @Override
    public TocReader getTocReader() {
        return null;
    }

    @Override
    public long getBytesConsumed() {
        return 0;
    }

    @Override
    public long getMaxEventId() throws IOException {
        return 0;
    }
}
