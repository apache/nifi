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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.ByteArraySchemaRecordWriter;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;

public class RecordWriters {
    private static final int DEFAULT_COMPRESSION_BLOCK_SIZE = 1024 * 1024; // 1 MB

    public static RecordWriter newSchemaRecordWriter(final File file, final AtomicLong idGenerator, final boolean compressed, final boolean createToc) throws IOException {
        return newSchemaRecordWriter(file, idGenerator, compressed, createToc, DEFAULT_COMPRESSION_BLOCK_SIZE);
    }

    public static RecordWriter newSchemaRecordWriter(final File file, final AtomicLong idGenerator, final boolean compressed, final boolean createToc,
        final int compressionBlockBytes) throws IOException {
        final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
        return new ByteArraySchemaRecordWriter(file, idGenerator, tocWriter, compressed, compressionBlockBytes);
    }

}
