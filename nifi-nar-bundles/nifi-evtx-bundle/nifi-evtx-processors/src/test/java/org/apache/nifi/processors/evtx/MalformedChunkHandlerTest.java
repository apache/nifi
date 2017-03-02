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

package org.apache.nifi.processors.evtx;

import com.google.common.net.MediaType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MalformedChunkHandlerTest {
    Relationship badChunkRelationship;

    MalformedChunkHandler malformedChunkHandler;

    @Before
    public void setup() {
        badChunkRelationship = new Relationship.Builder().build();
        malformedChunkHandler = new MalformedChunkHandler(badChunkRelationship);
    }

    @Test
    public void testHandle() {
        String name = "name";
        byte[] badChunk = {8};
        FlowFile original = mock(FlowFile.class);
        FlowFile updated1 = mock(FlowFile.class);
        FlowFile updated2 = mock(FlowFile.class);
        FlowFile updated3 = mock(FlowFile.class);
        FlowFile updated4 = mock(FlowFile.class);
        ProcessSession session = mock(ProcessSession.class);

        when(session.create(original)).thenReturn(updated1);
        when(session.putAttribute(updated1, CoreAttributes.FILENAME.key(), name)).thenReturn(updated2);
        when(session.putAttribute(updated2, CoreAttributes.MIME_TYPE.key(), MediaType.APPLICATION_BINARY.toString())).thenReturn(updated3);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        when(session.write(eq(updated3), any(OutputStreamCallback.class))).thenAnswer(invocation -> {
            ((OutputStreamCallback) invocation.getArguments()[1]).process(out);
            return updated4;
        });

        malformedChunkHandler.handle(original, session, name, badChunk);

        verify(session).transfer(updated4, badChunkRelationship);
        assertArrayEquals(badChunk, out.toByteArray());
    }
}
