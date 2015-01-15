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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipInputStream;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.util.FlowFilePackagerV1;
import org.apache.nifi.util.FlowFilePackagerV3;
import org.apache.nifi.util.ObjectHolder;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

/**
 * <p>
 * Attempts to detect the MIME Type of a FlowFile by examining its contents. If
 * the MIME Type is determined, it is added to an attribute with the name
 * mime.type
 * </p>
 *
 * <p>
 * The following MIME Types are supported:
 *
 * <ul>
 * <li>application/gzip</li>
 * <li>application/bzip2</li>
 * <li>application/flowfile-v3</li>
 * <li>application/flowfile-v1 (requires Identify TAR be set to true)</li>
 * <li>application/xml</li>
 * <li>video/mp4</li>
 * <li>video/x-m4v</li>
 * <li>video/mp4a-latm</li>
 * <li>video/quicktime</li>
 * <li>video/mpeg</li>
 * <li>audio/wav</li>
 * <li>audio/mp3</li>
 * <li>image/bmp</li>
 * <li>image/png</li>
 * <li>image/jpg</li>
 * <li>image/gif</li>
 * <li>image/tif</li>
 * <li>application/vnd.ms-works</li>
 * <li>application/msexcel</li>
 * <li>application/mspowerpoint</li>
 * <li>application/msaccess</li>
 * <li>application/x-ms-wmv</li>
 * <li>application/pdf</li>
 * <li>application/x-rpm</li>
 * <li>application/tar</li>
 * <li>application/x-7z-compressed</li>
 * <li>application/java-archive</li>
 * <li>application/zip</li>
 * <li>application/x-lzh</li>
 * </ul>
 * </p>
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"compression", "gzip", "bzip2", "zip", "MIME", "mime.type", "file", "identify"})
@CapabilityDescription("Attempts to identify the MIME Type used for a FlowFile. If the MIME Type can be identified, "
        + "an attribute with the name 'mime.type' is added with the value being the MIME Type. If the MIME Type cannot be determined, "
        + "the value will be set to 'application/octet-stream'. Some MIME Types require reading a significant amount of data; for these MIME Types, their identification "
        + "is optional. The algorithm may have to read the entire contents of the file for each type of identification.")
public class IdentifyMimeType extends AbstractProcessor {

    public static final PropertyDescriptor IDENTIFY_ZIP = new PropertyDescriptor.Builder()
            .name("Identify ZIP")
            .description("Determines whether or not to attempt in depth identification of ZIP MIME types")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor IDENTIFY_TAR = new PropertyDescriptor.Builder()
            .name("Identify TAR")
            .description("Determines whether or not to attempt in depth identification of TAR MIME types")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All FlowFiles are routed to success").build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final List<MagicHeader> magicHeaders;
    private final List<MagicHeader> zipMagicHeaders;
    private final List<MagicHeader> tarMagicHeaders;
    private final List<ContentScanningMimeTypeIdentifier> contentScanners;
    private final int magicHeaderMaxLength;

    public IdentifyMimeType() {
        // compile a list of Magic Header detectors
        final List<MagicHeader> headers = new ArrayList<>();
        headers.add(new SimpleMagicHeader("application/gzip", new byte[]{0x1f, (byte) 0x8b}));
        headers.add(new SimpleMagicHeader("application/bzip2", new byte[]{0x42, 0x5a}));
        headers.add(new SimpleMagicHeader("application/flowfile-v3", FlowFilePackagerV3.MAGIC_HEADER));
        headers.add(new SimpleMagicHeader("application/xml", new byte[]{0x3c, 0x3f, 0x78, 0x6d, 0x6c, 0x20}));
        headers.add(new SimpleMagicHeader("video/mp4", new byte[]{0, 0, 0, 0x14, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6F, 0x6D}));
        headers.add(new SimpleMagicHeader("video/mp4", new byte[]{0, 0, 0, 0x14, 0x66, 0x74, 0x79, 0x70, 0x33, 0x67, 0x70, 0x35}));
        headers.add(new SimpleMagicHeader("video/mp4", new byte[]{0, 0, 0, 0x14, 0x66, 0x74, 0x79, 0x70, 0X4d, 0X53, 0X4e, 0X56, 0X01, 0X29, 0, 0X46, 0X4d, 0X53, 0X4e, 0X56, 0X6d, 0X70, 0X34, 0X32}));
        headers.add(new SimpleMagicHeader("video/x-m4v", new byte[]{0, 0, 0, 0x18, 0x66, 0x74, 0x79, 0x70, 0x6D, 0x70, 0x34, 0x32}));
        headers.add(new SimpleMagicHeader("video/mp4a-latm", new byte[]{0, 0, 0, 0x18, 0x66, 0x74, 0x79, 0x70, 0x4D, 0x34, 0x41, 0x20}));
        headers.add(new SimpleMagicHeader("video/quicktime", new byte[]{0, 0, 0, 0x14, 0x66, 0x74, 0x79, 0x70, 0x71, 0x74, 0x20, 0x20}));
        headers.add(new SimpleMagicHeader("video/quicktime", new byte[]{0x6D, 0x6F, 0x6F, 0x76}, 4));
        headers.add(new SimpleMagicHeader("audio/mp3", new byte[]{0x49, 0x44, 0x33}));
        headers.add(new SimpleMagicHeader("image/bmp", new byte[]{0x42, 0x4D}));
        headers.add(new SimpleMagicHeader("image/png", new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}));
        headers.add(new SimpleMagicHeader("image/jpg", new byte[]{(byte) 0xFF, (byte) 0xD8, (byte) 0xFF}));
        headers.add(new SimpleMagicHeader("image/gif", new byte[]{0x47, 0x49, 0x46, 0x38, 0x37, 0x61}));
        headers.add(new SimpleMagicHeader("image/gif", new byte[]{0x47, 0x49, 0x46, 0x38, 0x39, 0x61}));
        headers.add(new SimpleMagicHeader("image/tif", new byte[]{0x49, 0x20, 0x49}));
        headers.add(new SimpleMagicHeader("image/tif", new byte[]{0x49, 0x49, 0x2A, 0x00}));
        headers.add(new SimpleMagicHeader("image/tif", new byte[]{0x4D, 0x4D, 0x00, 0x2A}));
        headers.add(new SimpleMagicHeader("image/tif", new byte[]{0x4D, 0x4D, 0x00, 0x2B}));
        headers.add(new SimpleMagicHeader("application/vnd.ms-works", new byte[]{(byte) 0xFF, 0x00, 0x02, 0x00, 0x04, 0x04, 0x05, 0x54, 0x02, 0x00}));
        headers.add(new SimpleMagicHeader("application/msexcel", new byte[]{0x09, 0x08, 0x10, 0, 0, 0x06, 0x05, 0}, 512));
        headers.add(new SimpleMagicHeader("application/mspowerpoint", new byte[]{0x00, 0x6E, 0x1E, (byte) 0xF0}, 512));
        headers.add(new SimpleMagicHeader("application/mspowerpoint", new byte[]{0x0F, 0x00, (byte) 0xE8, 0x03}, 512));
        headers.add(new SimpleMagicHeader("application/mspowerpoint", new byte[]{(byte) 0xA0, 0x46, 0x1D, (byte) 0xF0}, 512));
        headers.add(new CompoundMagicHeader("application/mspowerpoint",
                new SimpleMagicHeader("", new byte[]{(byte) 0xFD, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, 512),
                new SimpleMagicHeader("", new byte[]{0x00, 0x00, 0x00}, 517)));
        headers.add(new SimpleMagicHeader("application/msaccess", new byte[]{0x00, 0x01, 0x00, 0x00, 0x53, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x72, 0x64, 0x20, 0x41, 0x43, 0x45, 0x20, 0x44, 0x42}));
        headers.add(new SimpleMagicHeader("application/msaccess", new byte[]{0x00, 0x01, 0x00, 0x00, 0x53, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x72, 0x64, 0x20, 0x4A, 0x65, 0x74, 0x20, 0x44, 0x42}));
        for (byte b : new byte[]{0x10, 0x1F, 0x22, 0x23, 0x28, 0x29}) {
            headers.add(new SimpleMagicHeader("application/msaccess", new byte[]{(byte) 0xFD, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, b, 0x00}, 512));
            headers.add(new SimpleMagicHeader("application/msaccess", new byte[]{(byte) 0xFD, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, b, 0x02}, 512));
        }
        headers.add(new SimpleMagicHeader("application/x-ms-wmv", new byte[]{0x30, 0x26, (byte) 0xB2, 0x75, (byte) 0x8E, 0x66, (byte) 0xCF, 0x11, (byte) 0xA6, (byte) 0xD9, 0x00, (byte) 0xAA, 0x00, 0x62, (byte) 0xCE, 0x6C}));
        headers.add(new SimpleMagicHeader("application/pdf", new byte[]{0x25, 0x50, 0x44, 0x46}));
        headers.add(new SimpleMagicHeader("application/x-rpm", new byte[]{(byte) 0xED, (byte) 0xAB, (byte) 0xEE, (byte) 0xDB}));
        headers.add(new SimpleMagicHeader("application/x-7z-compressed", new byte[]{0x37, 0x7A, (byte) 0xBC, (byte) 0xAF, 0x27, 0x1C}));
        headers.add(new SimpleMagicHeader("application/java-archive", new byte[]{0x4A, 0x41, 0x52, 0x43, 0x53, 0x00}));
        headers.add(new SimpleMagicHeader("application/java-archive", new byte[]{0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x08}));
        headers.add(new SimpleMagicHeader("application/java-archive", new byte[]{0x50, 0x4B, 0x03, 0x04, (byte) 0xA0, 0x00, 0x00}));
        headers.add(new SimpleMagicHeader("application/x-lzh", new byte[]{0x2D, 0x6C, 0x68}, 2));
        headers.add(new CompoundMagicHeader("audio/wav",
                new SimpleMagicHeader("", new byte[]{0x52, 0x49, 0x46, 0x46}),
                new SimpleMagicHeader("", new byte[]{0x57, 0x41, 0x56, 0x45, 0x66, 0x6D, 0x74, 0x20}, 8)));
        for (int nibble = 0xB0; nibble <= 0xBF; nibble++) {
            headers.add(new SimpleMagicHeader("video/mpeg", new byte[]{0x00, 0x00, 0x01, (byte) nibble}));
        }
        this.magicHeaders = Collections.unmodifiableList(headers);

        // additional Magic Header detectors that will be turned off based on property settings
        final List<MagicHeader> zipHeaders = new ArrayList<>();
        zipHeaders.add(new SimpleMagicHeader("application/zip", new byte[]{0x50, 0x4B, 0x03, 0x04}));
        this.zipMagicHeaders = Collections.unmodifiableList(zipHeaders);
        final List<MagicHeader> tarHeaders = new ArrayList<>();
        tarHeaders.add(new SimpleMagicHeader("application/tar", new byte[]{0x75, 0x73, 0x74, 0x61, 0x72}, 257));
        this.tarMagicHeaders = Collections.unmodifiableList(tarHeaders);

        // determine the max length that we need to buffer for magic headers
        int max = 0;
        for (final MagicHeader header : magicHeaders) {
            max = Math.max(max, header.getRequiredBufferLength());
        }
        for (final MagicHeader header : zipMagicHeaders) {
            max = Math.max(max, header.getRequiredBufferLength());
        }
        for (final MagicHeader header : tarMagicHeaders) {
            max = Math.max(max, header.getRequiredBufferLength());
        }
        this.magicHeaderMaxLength = max;

        // create list of Content Scanners
        final List<ContentScanningMimeTypeIdentifier> scanningIdentifiers = new ArrayList<>();
        scanningIdentifiers.add(new ZipIdentifier());
        scanningIdentifiers.add(new TarIdentifier());
        this.contentScanners = Collections.unmodifiableList(scanningIdentifiers);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(IDENTIFY_ZIP);
        properties.add(IDENTIFY_TAR);
        this.properties = Collections.unmodifiableList(properties);
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
        final boolean identifyZip = context.getProperty(IDENTIFY_ZIP).asBoolean();
        final boolean identifyTar = context.getProperty(IDENTIFY_TAR).asBoolean();

        final ObjectHolder<String> mimeTypeRef = new ObjectHolder<>(null);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream stream) throws IOException {
                try (final InputStream in = new BufferedInputStream(stream)) {
                    // read in up to magicHeaderMaxLength bytes
                    in.mark(magicHeaderMaxLength);
                    byte[] header = new byte[magicHeaderMaxLength];
                    for (int i = 0; i < header.length; i++) {
                        final int next = in.read();
                        if (next >= 0) {
                            header[i] = (byte) next;
                        } else if (i == 0) {
                            header = new byte[0];
                        } else {
                            final byte[] newBuffer = new byte[i - 1];
                            System.arraycopy(header, 0, newBuffer, 0, i - 1);
                            header = newBuffer;
                            break;
                        }
                    }
                    in.reset();

                    for (final MagicHeader magicHeader : magicHeaders) {
                        if (magicHeader.matches(header)) {
                            mimeTypeRef.set(magicHeader.getMimeType());
                            return;
                        }
                    }

                    if (!identifyZip) {
                        for (final MagicHeader magicHeader : zipMagicHeaders) {
                            if (magicHeader.matches(header)) {
                                mimeTypeRef.set(magicHeader.getMimeType());
                                return;
                            }
                        }
                    }

                    if (!identifyTar) {
                        for (final MagicHeader magicHeader : tarMagicHeaders) {
                            if (magicHeader.matches(header)) {
                                mimeTypeRef.set(magicHeader.getMimeType());
                                return;
                            }
                        }
                    }
                }
            }
        });

        String mimeType = mimeTypeRef.get();
        if (mimeType == null) {
            for (final ContentScanningMimeTypeIdentifier scanningIdentifier : this.contentScanners) {
                if (scanningIdentifier.isEnabled(context)) {
                    session.read(flowFile, new InputStreamCallback() {
                        @Override
                        public void process(final InputStream in) throws IOException {
                            String mimeType = scanningIdentifier.getMimeType(in);
                            if (mimeType != null) {
                                mimeTypeRef.set(mimeType);
                            }
                        }
                    });

                    if (mimeTypeRef.get() != null) {
                        break;
                    }
                }
            }
        }

        mimeType = mimeTypeRef.get();
        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            logger.info("Unable to identify MIME Type for {}; setting to application/octet-stream", new Object[]{flowFile});
        } else {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
            logger.info("Identified {} as having MIME Type {}", new Object[]{flowFile, mimeType});
        }

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private static interface ContentScanningMimeTypeIdentifier {

        boolean isEnabled(ProcessContext context);

        String getMimeType(InputStream in) throws IOException;
    }

    private static class ZipIdentifier implements ContentScanningMimeTypeIdentifier {

        @Override
        public String getMimeType(final InputStream in) throws IOException {
            final ZipInputStream zipIn = new ZipInputStream(in);
            try {
                if (zipIn.getNextEntry() != null) {
                    return "application/zip";
                }
            } catch (final Exception e) {
            }
            return null;
        }

        @Override
        public boolean isEnabled(final ProcessContext context) {
            return context.getProperty(IDENTIFY_ZIP).asBoolean();
        }
    }

    private static class TarIdentifier implements ContentScanningMimeTypeIdentifier {

        @Override
        public String getMimeType(final InputStream in) throws IOException {
            try (final TarArchiveInputStream tarIn = new TarArchiveInputStream(in)) {
                final TarArchiveEntry firstEntry = tarIn.getNextTarEntry();
                if (firstEntry != null) {
                    if (firstEntry.getName().equals(FlowFilePackagerV1.FILENAME_ATTRIBUTES)) {
                        final TarArchiveEntry secondEntry = tarIn.getNextTarEntry();
                        if (secondEntry != null && secondEntry.getName().equals(FlowFilePackagerV1.FILENAME_CONTENT)) {
                            return "application/flowfile-v1";
                        }
                    }
                    return "application/tar";
                }
            } catch (final Exception e) {
            }
            return null;
        }

        @Override
        public boolean isEnabled(final ProcessContext context) {
            return context.getProperty(IDENTIFY_TAR).asBoolean();
        }
    }

    private static interface MagicHeader {

        int getRequiredBufferLength();

        String getMimeType();

        boolean matches(final byte[] header);
    }

    private static class SimpleMagicHeader implements MagicHeader {

        private final String mimeType;
        private final int offset;
        private final byte[] byteSequence;

        public SimpleMagicHeader(final String mimeType, final byte[] byteSequence) {
            this(mimeType, byteSequence, 0);
        }

        public SimpleMagicHeader(final String mimeType, final byte[] byteSequence, final int offset) {
            this.mimeType = mimeType;
            this.byteSequence = byteSequence;
            this.offset = offset;
        }

        @Override
        public int getRequiredBufferLength() {
            return byteSequence.length + offset;
        }

        @Override
        public String getMimeType() {
            return mimeType;
        }

        @Override
        public boolean matches(final byte[] header) {
            if (header.length < getRequiredBufferLength()) {
                return false;
            }

            for (int i = 0; i < byteSequence.length; i++) {
                if (byteSequence[i] != header[offset + i]) {
                    return false;
                }
            }

            return true;
        }
    }

    private static class CompoundMagicHeader implements MagicHeader {

        private final MagicHeader[] headers;
        private final int requiredLength;
        private final String mimeType;

        public CompoundMagicHeader(final String mimeType, final MagicHeader... headers) {
            this.mimeType = mimeType;
            this.headers = headers;

            int max = 0;
            for (final MagicHeader header : headers) {
                max = Math.max(max, header.getRequiredBufferLength());
            }

            this.requiredLength = max;
        }

        @Override
        public int getRequiredBufferLength() {
            return requiredLength;
        }

        @Override
        public String getMimeType() {
            return mimeType;
        }

        @Override
        public boolean matches(final byte[] header) {
            for (final MagicHeader mh : headers) {
                if (!mh.matches(header)) {
                    return false;
                }
            }

            return true;
        }

    }
}
