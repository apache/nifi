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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.hadoop.util.OutputStreamWritable;
import org.apache.nifi.processors.hadoop.util.SequenceFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This class reads a SequenceFile and generates FlowFiles, one per each KeyValue Pair in the SequenceFile. The FlowFile name is the key, which is typically a file name but may not be; the FlowFile
 * content is the value.
 *
 */
public class ValueReader implements SequenceFileReader<Set<FlowFile>> {

    private static final Logger LOG = LoggerFactory.getLogger(ValueReader.class);
    // pattern that starts with any letter or '/'...looking for file names
    private static final Pattern LOOKS_LIKE_FILENAME = Pattern.compile("^[\\w/].*");
    private final ProcessSession session;

    public ValueReader(ProcessSession session) {
        this.session = session;
    }

    @Override
    public Set<FlowFile> readSequenceFile(final Path file, Configuration configuration, FileSystem fileSystem) throws IOException {

        Set<FlowFile> flowFiles = new HashSet<>();
        final SequenceFile.Reader reader = new SequenceFile.Reader(configuration, Reader.file(fileSystem.makeQualified(file)));
        final String inputfileName = file.getName() + "." + System.nanoTime() + ".";
        int counter = 0;
        LOG.debug("Reading from sequence file {}", new Object[]{file});
        final OutputStreamWritableCallback writer = new OutputStreamWritableCallback(reader);
        Text key = new Text();
        try {
            while (reader.next(key)) {
                String fileName = key.toString();
                // the key may be a file name, and may not
                if (LOOKS_LIKE_FILENAME.matcher(fileName).matches()) {
                    if (fileName.contains(File.separator)) {
                        fileName = StringUtils.substringAfterLast(fileName, File.separator);
                    }
                    fileName = fileName + "." + System.nanoTime();
                } else {
                    fileName = inputfileName + ++counter;
                }
                FlowFile flowFile = session.create();
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
                try {
                    flowFile = session.write(flowFile, writer);
                    flowFiles.add(flowFile);
                } catch (ProcessException e) {
                    LOG.error("Could not write to flowfile {}", new Object[]{flowFile}, e);
                    session.remove(flowFile);
                }
                key.clear();
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }

        return flowFiles;
    }

    private static final class OutputStreamWritableCallback implements OutputStreamCallback {

        private final Reader reader;

        private OutputStreamWritableCallback(Reader reader) {
            this.reader = reader;
        }

        @Override
        public void process(OutputStream out) throws IOException {
            final OutputStreamWritable fileData = new OutputStreamWritable(out, false);
            reader.getCurrentValue(fileData);
        }
    }

}
