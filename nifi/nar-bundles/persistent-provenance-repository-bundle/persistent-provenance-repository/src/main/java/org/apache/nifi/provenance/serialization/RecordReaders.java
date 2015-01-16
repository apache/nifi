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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.provenance.StandardRecordReader;
import org.apache.nifi.provenance.lucene.LuceneUtil;

public class RecordReaders {

    public static RecordReader newRecordReader(File file, final Collection<Path> provenanceLogFiles) throws IOException {
        if (!file.exists()) {
            if (provenanceLogFiles == null) {
                throw new FileNotFoundException(file.toString());
            }

            final String baseName = LuceneUtil.substringBefore(file.getName(), ".") + ".";
            for (final Path path : provenanceLogFiles) {
                if (path.toFile().getName().startsWith(baseName)) {
                    file = path.toFile();
                    break;
                }
            }
        }

        if (file == null || !file.exists()) {
            throw new FileNotFoundException(file.toString());
        }

        final InputStream fis = new FileInputStream(file);
        final InputStream readableStream;
        if (file.getName().endsWith(".gz")) {
            readableStream = new BufferedInputStream(new GZIPInputStream(fis));
        } else {
            readableStream = new BufferedInputStream(fis);
        }

        final DataInputStream dis = new DataInputStream(readableStream);
        @SuppressWarnings("unused")
        final String repoClassName = dis.readUTF();
        final int serializationVersion = dis.readInt();

        return new StandardRecordReader(dis, serializationVersion, file.getName());
    }

}
