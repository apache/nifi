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

import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestFlowFileAccessInputStream {

    private InputStream in;
    private FlowFile flowFile;
    private ContentClaim claim;
    private final byte[] data = new byte[16];

    @BeforeEach
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
            public Integer answer(InvocationOnMock invocation) {
                if (count == 0) {
                    count++;
                    return 16;
                }
                return -1;
            }
        });
        // Flow file total size is 32
        Mockito.when(flowFile.getSize()).thenReturn(32L);

        FlowFileAccessInputStream flowFileAccessInputStream = new FlowFileAccessInputStream(in, flowFile, claim);

        ContentNotFoundException contentNotFoundException =
                assertThrows(ContentNotFoundException.class, () -> {
                    while (flowFileAccessInputStream.read(data) != -1){}
                });

        assertTrue(contentNotFoundException.getMessage().contains("Stream contained only 16 bytes" +
                " but should have contained 32"), "ContentNotFoundException message not matched.");
        flowFileAccessInputStream.close();
    }
}
