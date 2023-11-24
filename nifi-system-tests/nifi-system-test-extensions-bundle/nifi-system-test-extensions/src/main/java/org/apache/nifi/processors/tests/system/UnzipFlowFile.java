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
package org.apache.nifi.processors.tests.system;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipFlowFile extends AbstractProcessor {

    private final Relationship REL_UNZIPPED = new Relationship.Builder()
        .name("unzipped")
        .build();

    private final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .autoTerminateDefault(true)
        .build();

    private final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<>(Arrays.asList(REL_UNZIPPED, REL_ORIGINAL, REL_FAILURE));
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<FlowFile> created = new ArrayList<>();
        try (final InputStream in = session.read(flowFile);
             final ZipInputStream zipInputStream = new ZipInputStream(in)) {

            while (true) {
                final ZipEntry zipEntry = zipInputStream.getNextEntry();
                if (zipEntry == null) {
                    break;
                }

                final String filename = zipEntry.getName();
                FlowFile outFile = session.create(flowFile);
                outFile = session.putAttribute(outFile, "filename", filename);
                created.add(outFile);

                session.write(outFile, out -> {
                    StreamUtils.copy(zipInputStream, out);
                });
            }

        } catch (final IOException e) {
            getLogger().error("Failed to unzip {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            session.remove(created);
            return;
        }

        session.transfer(created, REL_UNZIPPED);
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
