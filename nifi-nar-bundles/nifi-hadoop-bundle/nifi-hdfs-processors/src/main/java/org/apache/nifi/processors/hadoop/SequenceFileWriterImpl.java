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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.hadoop.util.ByteFilteringOutputStream;
import org.apache.nifi.processors.hadoop.util.InputStreamWritable;
import org.apache.nifi.processors.hadoop.util.SequenceFileWriter;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

public class SequenceFileWriterImpl implements SequenceFileWriter {

    protected static Logger logger = LoggerFactory.getLogger(SequenceFileWriterImpl.class);

    @Override
    public FlowFile writeSequenceFile(final FlowFile flowFile, final ProcessSession session,
            final Configuration configuration, final CompressionType compressionType) {

        if (flowFile.getSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot write " + flowFile
                    + "to Sequence File because its size is greater than the largest possible Integer");
        }
        final String sequenceFilename = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".sf";

        // Analytics running on HDFS want data that is written with a BytesWritable. However, creating a
        // BytesWritable requires that we buffer the entire file into memory in a byte array.
        // We can create an FSFilterableOutputStream to wrap the FSDataOutputStream and use that to replace
        // the InputStreamWritable class name with the BytesWritable class name when we write the header.
        // This allows the Sequence File to say that the Values are of type BytesWritable (so they can be
        // read via the BytesWritable class) while allowing us to stream the data rather than buffering
        // entire files in memory.
        final byte[] toReplace, replaceWith;
        try {
            toReplace = InputStreamWritable.class.getCanonicalName().getBytes("UTF-8");
            replaceWith = BytesWritable.class.getCanonicalName().getBytes("UTF-8");
        } catch (final UnsupportedEncodingException e) {
            // This won't happen.
            throw new RuntimeException("UTF-8 is not a supported Character Format");
        }

        final StopWatch watch = new StopWatch(true);
        FlowFile sfFlowFile = session.write(flowFile, new StreamCallback() {

            @Override
            public void process(InputStream in, OutputStream out) throws IOException {
                // Use a FilterableOutputStream to change 'InputStreamWritable' to 'BytesWritable' - see comment
                // above for an explanation of why we want to do this.
                final ByteFilteringOutputStream bwos = new ByteFilteringOutputStream(out);

                // TODO: Adding this filter could be dangerous... A Sequence File's header contains 3 bytes: "SEQ",
                // followed by 1 byte that is the Sequence File version, followed by 2 "entries." These "entries"
                // contain the size of the Key/Value type and the Key/Value type. So, we will be writing the
                // value type as InputStreamWritable -- which we need to change to BytesWritable. This means that
                // we must also change the "size" that is written, but replacing this single byte could be
                // dangerous. However, we know exactly what will be written to the header, and we limit this at one
                // replacement, so we should be just fine.
                bwos.addFilter(toReplace, replaceWith, 1);
                bwos.addFilter((byte) InputStreamWritable.class.getCanonicalName().length(),
                        (byte) BytesWritable.class.getCanonicalName().length(), 1);

                try (final FSDataOutputStream fsDataOutputStream = new FSDataOutputStream(bwos, new Statistics(""));
                        final SequenceFile.Writer writer = SequenceFile.createWriter(configuration,
                                SequenceFile.Writer.stream(fsDataOutputStream),
                                SequenceFile.Writer.keyClass(Text.class),
                                SequenceFile.Writer.valueClass(InputStreamWritable.class),
                                SequenceFile.Writer.compression(compressionType, new DefaultCodec()))) {

                    processInputStream(in, flowFile, writer);

                } finally {
                    watch.stop();
                }
            }
        });
        logger.debug("Wrote Sequence File {} ({}).",
                new Object[]{sequenceFilename, watch.calculateDataRate(flowFile.getSize())});
        return sfFlowFile;
    }

    protected void processInputStream(InputStream stream, FlowFile flowFile, final Writer writer) throws IOException {
        int fileSize = (int) flowFile.getSize();
        final InputStreamWritable inStreamWritable = new InputStreamWritable(new BufferedInputStream(stream), fileSize);
        String key = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        writer.append(new Text(key), inStreamWritable);
    }
}
