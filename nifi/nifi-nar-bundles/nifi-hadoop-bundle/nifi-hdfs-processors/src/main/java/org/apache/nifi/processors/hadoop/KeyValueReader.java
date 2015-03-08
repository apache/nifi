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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

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

/**
 * This class reads a SequenceFile and generates FlowFiles, one per KeyValue
 * pair in the SequenceFile. The FlowFile name is based on the the incoming file
 * name with System nanotime appended; the FlowFile content is the key/value
 * pair serialized via Text.
 */
public class KeyValueReader implements SequenceFileReader<Set<FlowFile>> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueReader.class);
    private final ProcessSession session;
    // pattern that starts with any letter or '/'...looking for file names
    private static final Pattern LOOKS_LIKE_FILENAME = Pattern.compile("^[\\w/].*");

    public KeyValueReader(ProcessSession session) {
        this.session = session;
    }

    @Override
    public Set<FlowFile> readSequenceFile(Path file, Configuration configuration, FileSystem fileSystem) throws IOException {

        final SequenceFile.Reader reader;

        Set<FlowFile> flowFiles = new HashSet<>();
        reader = new SequenceFile.Reader(fileSystem, file, configuration);
        final Text key = new Text();
        final KeyValueWriterCallback callback = new KeyValueWriterCallback(reader);
        final String inputfileName = file.getName() + "." + System.nanoTime() + ".";
        int counter = 0;
        LOG.debug("Read from SequenceFile: {} ", new Object[]{file});
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
                callback.key = key;
                try {
                    flowFile = session.write(flowFile, callback);
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

    /**
     * Serializes the key and value and writes to the flow file's output stream.
     *
     * @author unattributed
     *
     */
    static class KeyValueWriterCallback implements OutputStreamCallback {

        Text key;
        private final Reader reader;

        public KeyValueWriterCallback(Reader reader) {
            this.reader = reader;
        }

        @Override
        public void process(OutputStream out) throws IOException {

            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(key.getLength());
            dos.write(key.getBytes(), 0, key.getLength());
            final OutputStreamWritable fileData = new OutputStreamWritable(out, true);
            reader.getCurrentValue(fileData);
        }

    }
}
