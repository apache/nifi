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
package org.apache.nifi.processors.hadoop;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.hadoop.util.InputStreamWritable;
import org.slf4j.LoggerFactory;

public class ZipUnpackerSequenceFileWriter extends SequenceFileWriterImpl {

    static {
        logger = LoggerFactory.getLogger(ZipUnpackerSequenceFileWriter.class);
    }

    @Override
    protected void processInputStream(InputStream stream, final FlowFile flowFile, final Writer writer) throws IOException {

        try (final ZipInputStream zipIn = new ZipInputStream(new BufferedInputStream(stream))) {
            ZipEntry zipEntry;
            while ((zipEntry = zipIn.getNextEntry()) != null) {
                if (zipEntry.isDirectory()) {
                    continue;
                }
                final File file = new File(zipEntry.getName());
                final String key = file.getName();
                long fileSize = zipEntry.getSize();
                final InputStreamWritable inStreamWritable = new InputStreamWritable(zipIn, (int) fileSize);
                writer.append(new Text(key), inStreamWritable);
                logger.debug("Appending FlowFile {} to Sequence File", new Object[]{key});
            }
        }
    }
}
