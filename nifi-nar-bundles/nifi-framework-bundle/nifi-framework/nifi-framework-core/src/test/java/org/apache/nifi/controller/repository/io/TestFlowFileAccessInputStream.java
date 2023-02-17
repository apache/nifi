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
package org.apache.nifi.controller.repository.io;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFlowFileAccessInputStream {

    private InputStream in;
    private FlowFile flowFile;
    private ContentClaim claim;
    private final byte[] data = new byte[16];

    @Before
    public void setup() throws IOException {
        in = mock(InputStream.class);
        flowFile = mock(FlowFile.class);
        claim = mock(ContentClaim.class);
    }

    @Test
    public void testThrowExceptionWhenLessContentReadFromFlowFile() throws Exception {
        // Read only 16 bytes
        Mockito.when(in.read(data, 0, 16)).thenAnswer(new Answer<Integer>() {
            private int count = 0;

            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                if (count == 0) {
                    count++;
                    return 16;
                }
                return -1;
            }
        });
        // Flow file total size is 32
        Mockito.when(flowFile.getSize()).thenReturn(32l);

        FlowFileAccessInputStream flowFileAccessInputStream = new FlowFileAccessInputStream(in, flowFile, claim);
        try {
            while (flowFileAccessInputStream.read(data) != -1) {
            }
            fail("Should throw ContentNotFoundException when lesser bytes read from flow file.");
        } catch (ContentNotFoundException e) {
            assertTrue("ContentNotFoundException message not matched.", e.getMessage().contains("Stream contained only 16 bytes but should have contained 32"));
        } finally {
            flowFileAccessInputStream.close();
        }
    }
}
