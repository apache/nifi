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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32InputStream;
import org.apache.commons.codec.binary.Base32OutputStream;

import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.util.ValidatingBase32InputStream;
import org.apache.nifi.processors.standard.util.ValidatingBase64InputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"experimental", "encode", "decode", "base64", "hex"})
@CapabilityDescription("Encodes the FlowFile content in base64")
public class EncodeContent extends AbstractProcessor {

    public static final String ENCODE_MODE = "Encode";
    public static final String DECODE_MODE = "Decode";

    // List of support encodings.
    public static final String BASE64_ENCODING = "base64";
    public static final String BASE32_ENCODING = "base32";
    public static final String HEX_ENCODING = "hex";

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encoded or decoded")
            .required(true)
            .allowableValues(ENCODE_MODE, DECODE_MODE)
            .defaultValue(ENCODE_MODE)
            .build();

    public static final PropertyDescriptor ENCODING = new PropertyDescriptor.Builder()
            .name("Encoding")
            .description("Specifies the type of encoding used")
            .required(true)
            .allowableValues(BASE64_ENCODING, BASE32_ENCODING, HEX_ENCODING)
            .defaultValue(BASE64_ENCODING)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully encoded or decoded will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be encoded or decoded will be routed to failure")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(MODE);
        props.add(ENCODING);
        this.properties = Collections.unmodifiableList(props);

        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();

        boolean encode = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCODE_MODE);
        String encoding = context.getProperty(ENCODING).getValue();
        StreamCallback encoder = null;

        // Select the encoder/decoder to use
        if (encode) {
            if (encoding.equalsIgnoreCase(BASE64_ENCODING)) {
                encoder = new EncodeBase64();
            } else if (encoding.equalsIgnoreCase(BASE32_ENCODING)) {
                encoder = new EncodeBase32();
            } else if (encoding.equalsIgnoreCase(HEX_ENCODING)) {
                encoder = new EncodeHex();
            }
        } else {
            if (encoding.equalsIgnoreCase(BASE64_ENCODING)) {
                encoder = new DecodeBase64();
            } else if (encoding.equalsIgnoreCase(BASE32_ENCODING)) {
                encoder = new DecodeBase32();
            } else if (encoding.equalsIgnoreCase(HEX_ENCODING)) {
                encoder = new DecodeHex();
            }
        }

        if (encoder == null) {
            logger.warn("Unknown operation: {} {}", new Object[]{encode ? "encode" : "decode", encoding});
            return;
        }

        try {
            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, encoder);

            logger.info("Successfully {} {}", new Object[]{encode ? "encoded" : "decoded", flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.error("Failed to {} {} due to {}", new Object[]{encode ? "encode" : "decode", flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private class EncodeBase64 implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (Base64OutputStream bos = new Base64OutputStream(out)) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private class DecodeBase64 implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (Base64InputStream bis = new Base64InputStream(new ValidatingBase64InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private class EncodeBase32 implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (Base32OutputStream bos = new Base32OutputStream(out)) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private class DecodeBase32 implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try (Base32InputStream bis = new Base32InputStream(new ValidatingBase32InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private static final byte[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private class EncodeHex implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[8192];
            byte[] outBuf = new byte[inBuf.length * 2];
            while ((len = in.read(inBuf)) > 0) {
                for (int i = 0; i < len; i++) {
                    outBuf[i * 2] = HEX_CHARS[(inBuf[i] & 0xF0) >>> 4];
                    outBuf[i * 2 + 1] = HEX_CHARS[inBuf[i] & 0x0F];
                }
                out.write(outBuf, 0, len * 2);
            }
            out.flush();
        }
    }

    private class DecodeHex implements StreamCallback {

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[8192];
            Hex h = new Hex();
            while ((len = in.read(inBuf)) > 0) {
                // If the input buffer is of odd length, try to get another byte
                if (len % 2 != 0) {
                    int b = in.read();
                    if (b != -1) {
                        inBuf[len] = (byte) b;
                        len++;
                    }
                }

                // Construct a new buffer bounded to len
                byte[] slice = Arrays.copyOfRange(inBuf, 0, len);
                try {
                    out.write(h.decode(slice));
                } catch (DecoderException ex) {
                    throw new IOException(ex);
                }
            }
            out.flush();
        }
    }
}
