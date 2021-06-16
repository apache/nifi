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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.util.TextLineDemarcator;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SplitByLine extends AbstractProcessor {

    static final PropertyDescriptor USE_CLONE = new PropertyDescriptor.Builder()
        .name("Use Clone")
        .description("Whether or not to use session.clone for generating children FlowFiles")
        .required(true)
        .defaultValue("true")
        .allowableValues("true", "false")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.singletonList(USE_CLONE);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final boolean clone = context.getProperty(USE_CLONE).asBoolean();
        if (clone) {
            splitByClone(session, flowFile);
        } else {
            splitByWrite(session, flowFile);
        }

        session.remove(flowFile);
    }

    private void splitByClone(ProcessSession session, FlowFile flowFile) {
        final List<TextLineDemarcator.OffsetInfo> offsetInfos = new ArrayList<>();

        try (final InputStream in = session.read(flowFile);
             final TextLineDemarcator demarcator = new TextLineDemarcator(in)) {

            TextLineDemarcator.OffsetInfo offsetInfo;
            while ((offsetInfo = demarcator.nextOffsetInfo()) != null) {
                offsetInfos.add(offsetInfo);
            }
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        for (final TextLineDemarcator.OffsetInfo offsetInfo : offsetInfos) {
            FlowFile child = session.clone(flowFile, offsetInfo.getStartOffset(), offsetInfo.getLength() - offsetInfo.getCrlfLength());
            session.putAttribute(child, "num.lines", String.valueOf(offsetInfos.size()));
            session.transfer(child, REL_SUCCESS);
        }
    }

    private void splitByWrite(ProcessSession session, FlowFile flowFile) {
        final List<FlowFile> children = new ArrayList<>();
        try (final InputStream in = session.read(flowFile);
             final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {

            String line;
            while ((line = reader.readLine()) != null) {
                FlowFile child = session.create(flowFile);
                children.add(child);

                try (final OutputStream out = session.write(child)) {
                    final byte[] lineBytes = line.getBytes(StandardCharsets.UTF_8);
                    out.write(lineBytes);
                }
            }
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        for (FlowFile child : children) {
            session.putAttribute(child, "num.lines", String.valueOf(children.size()));
        }

        session.transfer(children, REL_SUCCESS);
    }
}
