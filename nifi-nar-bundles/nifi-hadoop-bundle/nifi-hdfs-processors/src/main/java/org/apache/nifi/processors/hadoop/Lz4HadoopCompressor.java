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
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

/**
 * This class allows NiFi to generate Lz4-compressed data that may be loaded to HDFS.
 * <p>
 * Per https://issues.apache.org/jira/browse/NIFI-3420 data compressed
 * using the Lz4 CLI is not readable by the native Hadoop Lz4 codec. This
 * processor adds the ability to convert data into Hadoop-readable block compressed Lz4
 * data by using the actual Hadoop Lz4 codec to do the compression.
 */
@SideEffectFree
@Tags({"Lz4", "Hadoop", "HDFS"})
@CapabilityDescription("Compress data as block compressed lz4.")
public class Lz4HadoopCompressor extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Lz4 Buffer Size")
            .displayName("Lz4 Buffer Size")
            .defaultValue("" + 64 * 1024)
            .description("The size of the buffer to use for lz4 decompression, default 64k")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BUFFER_SIZE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final int bufferSize = context.getProperty(BUFFER_SIZE).evaluateAttributeExpressions(flowFile).asInteger();

        // Raw buffer to use for reading from the input stream -> passed to block compression algorithm
        final byte[] rawBuffer = new byte[bufferSize];

        try {
            final StopWatch stopWatch = new StopWatch(true);

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (final BufferedInputStream reader = new BufferedInputStream(in, bufferSize);
                         final BufferedOutputStream writer = new BufferedOutputStream(out, bufferSize);
                         final LZ4BlockOutputStream compressionStream = new LZ4BlockOutputStream(writer, bufferSize)) {
                        int bytesRead;

                        while ((bytesRead = reader.read(rawBuffer, 0, bufferSize)) != -1) {
                            // Write the output of the compression
                            compressionStream.write(rawBuffer, 0, bytesRead);
                            compressionStream.flush();
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        getLogger().error("Failed to output compressed data");
                    }
                }
            });

            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            getLogger().info("Succesfully compressed bytes to lz4");
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
