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
package org.apache.nifi.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.text.StringEscapeUtils;

public class FlowFilePackagerV1 implements FlowFilePackager {

    public static final String FILENAME_ATTRIBUTES = "flowfile.attributes";
    public static final String FILENAME_CONTENT = "flowfile.content";
    public static final int DEFAULT_TAR_PERMISSIONS = 0644;

    private final int tarPermissions;

    public FlowFilePackagerV1() {
        this(DEFAULT_TAR_PERMISSIONS);
    }

    public FlowFilePackagerV1(final int tarPermissions) {
        this.tarPermissions = tarPermissions;
    }

    @Override
    public void packageFlowFile(final InputStream in, final OutputStream out, final Map<String, String> attributes, final long fileSize) throws IOException {
        try (final TarArchiveOutputStream tout = new TarArchiveOutputStream(out)) {
            writeAttributesEntry(attributes, tout);
            writeContentEntry(tout, in, fileSize);
            tout.finish();
            tout.flush();
            tout.close();
        }
    }

    private void writeAttributesEntry(final Map<String, String> attributes, final TarArchiveOutputStream tout) throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><!DOCTYPE properties\n  SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n");
        sb.append("<properties>");
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String escapedKey = StringEscapeUtils.escapeXml11(entry.getKey());
            final String escapedValue = StringEscapeUtils.escapeXml11(entry.getValue());
            sb.append("\n  <entry key=\"").append(escapedKey).append("\">").append(escapedValue).append("</entry>");
        }
        sb.append("</properties>");

        final byte[] metaBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        final TarArchiveEntry attribEntry = new TarArchiveEntry(FILENAME_ATTRIBUTES);
        attribEntry.setMode(tarPermissions);
        attribEntry.setSize(metaBytes.length);
        tout.putArchiveEntry(attribEntry);
        tout.write(metaBytes);
        tout.closeArchiveEntry();
    }

    private void writeContentEntry(final TarArchiveOutputStream tarOut, final InputStream inStream, final long fileSize) throws IOException {
        final TarArchiveEntry entry = new TarArchiveEntry(FILENAME_CONTENT);
        entry.setMode(tarPermissions);
        entry.setSize(fileSize);
        tarOut.putArchiveEntry(entry);
        final byte[] buffer = new byte[512 << 10];//512KB
        int bytesRead = 0;
        while ((bytesRead = inStream.read(buffer)) != -1) { //still more data to read
            if (bytesRead > 0) {
                tarOut.write(buffer, 0, bytesRead);
            }
        }

        copy(inStream, tarOut);
        tarOut.closeArchiveEntry();
    }

    public static long copy(final InputStream source, final OutputStream destination) throws IOException {
        final byte[] buffer = new byte[8192];
        int len;
        long totalCount = 0L;
        while ((len = source.read(buffer)) > 0) {
            destination.write(buffer, 0, len);
            totalCount += len;
        }
        return totalCount;
    }

}
