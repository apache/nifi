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

package org.apache.nifi.processors.azure;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAbstractAzureBlobProcessor_v12 {

    @Test
    public void testGetBlobName() {
        final AbstractAzureBlobProcessor_v12 processor = new AbstractAzureBlobProcessor_v12() {
            @Override
            protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return Collections.singletonList(BLOB_NAME);
            }

            @Override
            public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
            }
        };

        final MockProcessContext context = new MockProcessContext(processor);
        context.setProperty(AbstractAzureBlobProcessor_v12.BLOB_NAME, "${path}/${filename}");

        final MockFlowFile flowFile = new MockFlowFile(1);

        final String filename = "file1.txt";

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", filename);
        flowFile.putAttributes(attributes);
        flowFile.removeAttributes(Collections.singleton("path"));

        assertEquals(filename, processor.getBlobName(context, flowFile));

        attributes.put("path", ".");
        flowFile.putAttributes(attributes);
        assertEquals(filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "./");
        flowFile.putAttributes(attributes);
        assertEquals(filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "/");
        flowFile.putAttributes(attributes);
        assertEquals(filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "./a/b/c");
        flowFile.putAttributes(attributes);
        assertEquals("a/b/c/" + filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "a");
        flowFile.putAttributes(attributes);
        assertEquals("a/" + filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "a/b/c");
        flowFile.putAttributes(attributes);
        assertEquals("a/b/c/" + filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "/a");
        flowFile.putAttributes(attributes);
        assertEquals("a/" + filename, processor.getBlobName(context, flowFile));

        attributes.put("path", "/");
        flowFile.putAttributes(attributes);
        assertEquals(filename, processor.getBlobName(context, flowFile));
    }

}
