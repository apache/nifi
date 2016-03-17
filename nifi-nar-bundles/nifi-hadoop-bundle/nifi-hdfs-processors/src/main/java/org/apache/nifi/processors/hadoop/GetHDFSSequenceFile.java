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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.hadoop.util.SequenceFileReader;
import org.apache.nifi.util.StopWatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This processor is used to pull files from HDFS. The files being pulled in MUST be SequenceFile formatted files. The processor creates a flow file for each key/value entry in the ingested
 * SequenceFile. The created flow file's content depends on the value of the optional configuration property FlowFile Content. Currently, there are two choices: VALUE ONLY and KEY VALUE PAIR. With the
 * prior, only the SequenceFile value element is written to the flow file contents. With the latter, the SequenceFile key and value are written to the flow file contents as serialized objects; the
 * format is key length (int), key(String), value length(int), value(bytes). The default is VALUE ONLY.
 * <p>
 * NOTE: This processor loads the entire value entry into memory. While the size limit for a value entry is 2GB, this will cause memory problems if there are too many concurrent tasks and the data
 * being ingested is large.
 *
 */
@TriggerWhenEmpty
@Tags({"hadoop", "HDFS", "get", "fetch", "ingest", "source", "sequence file"})
@CapabilityDescription("Fetch sequence files from Hadoop Distributed File System (HDFS) into FlowFiles")
@SeeAlso(PutHDFS.class)
public class GetHDFSSequenceFile extends GetHDFS {

    static final String VALUE_ONLY = "VALUE ONLY";

    static final PropertyDescriptor FLOWFILE_CONTENT = new PropertyDescriptor.Builder()
            .name("FlowFile Content")
            .description("Indicate if the content is to be both the key and value of the Sequence File, or just the value.")
            .allowableValues(VALUE_ONLY, "KEY VALUE PAIR")
            .defaultValue(VALUE_ONLY)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> someProps = new ArrayList<>(super.getSupportedPropertyDescriptors());
        someProps.add(FLOWFILE_CONTENT);
        return Collections.unmodifiableList(someProps);
    }

    @Override
    protected void processBatchOfFiles(final List<Path> files, final ProcessContext context, final ProcessSession session) {
        final Configuration conf = getConfiguration();
        final FileSystem hdfs = getFileSystem();
        final String flowFileContentValue = context.getProperty(FLOWFILE_CONTENT).getValue();
        final boolean keepSourceFiles = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final Double bufferSizeProp = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B);
        if (bufferSizeProp != null) {
            int bufferSize = bufferSizeProp.intValue();
            conf.setInt(BUFFER_SIZE_KEY, bufferSize);
        }
        ProcessorLog logger = getLogger();
        final SequenceFileReader<Set<FlowFile>> reader;
        if (flowFileContentValue.equalsIgnoreCase(VALUE_ONLY)) {
            reader = new ValueReader(session);
        } else {
            reader = new KeyValueReader(session);
        }
        Set<FlowFile> flowFiles = Collections.emptySet();
        for (final Path file : files) {
            if (!this.isScheduled()) {
                break; // This processor should stop running immediately.
            }

            final StopWatch stopWatch = new StopWatch(false);
            try {
                stopWatch.start();
                if (!hdfs.exists(file)) {
                    continue; // If file is no longer here move on.
                }
                logger.debug("Reading file");
                flowFiles = reader.readSequenceFile(file, conf, hdfs);
                if (!keepSourceFiles && !hdfs.delete(file, false)) {
                    logger.warn("Unable to delete path " + file.toString() + " from HDFS.  Will likely be picked up over and over...");
                }
            } catch (Throwable t) {
                logger.error("Error retrieving file {} from HDFS due to {}", new Object[]{file, t});
                session.rollback();
                context.yield();
            } finally {
                stopWatch.stop();
                long totalSize = 0;
                for (FlowFile flowFile : flowFiles) {
                    totalSize += flowFile.getSize();
                    session.getProvenanceReporter().receive(flowFile, file.toString());
                }
                if (totalSize > 0) {
                    final String dataRate = stopWatch.calculateDataRate(totalSize);
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    logger.info("Created {} flowFiles from SequenceFile {}. Ingested in {} milliseconds at a rate of {}", new Object[]{
                        flowFiles.size(), file.toUri().toASCIIString(), millis, dataRate});
                    logger.info("Transferred flowFiles {}  to success", new Object[]{flowFiles});
                    session.transfer(flowFiles, REL_SUCCESS);
                }
            }
        }

    }

}
