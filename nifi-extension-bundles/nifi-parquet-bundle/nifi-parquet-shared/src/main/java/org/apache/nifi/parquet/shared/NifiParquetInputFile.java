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
package org.apache.nifi.parquet.shared;

import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.InputStream;

public class NifiParquetInputFile implements InputFile {

    private final long length;
    private final ByteCountingInputStream input;

    public NifiParquetInputFile(final InputStream input, final long length) {
        if (input == null) {
            throw new IllegalArgumentException("InputStream is required");
        }

        if (!input.markSupported()) {
            throw new IllegalArgumentException("InputStream must support mark/reset to be used with NifiParquetInputFile");
        }

        this.input = new ByteCountingInputStream(input);
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new NifiSeekableInputStream(input);
    }
}
