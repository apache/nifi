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
package org.apache.nifi.controller.queue.clustered.util;

import org.apache.nifi.controller.queue.clustered.client.async.nio.PeerChannel;
import org.apache.nifi.controller.queue.clustered.dto.PartitionStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

class TestMarshallingUtil {

    @Test
    public void testAfterWritingAndReadingStatusRemainsTheSame() throws Exception {
        final long totalSizeBytes = 1L;
        final int objectCount = 2;
        final int flowFilesOut = 3;
        final PartitionStatus status = new PartitionStatus(objectCount, totalSizeBytes, flowFilesOut);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        MarshallingUtil.writePartitionStatus(outputStream, status);

        final InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final PeerChannel channel = Mockito.mock(PeerChannel.class);
        Mockito.when(channel.readBytes(Mockito.anyInt())).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return inputStream.readNBytes(invocationOnMock.getArgument(0));
            }
        });

        final PartitionStatus result = MarshallingUtil.readPartitionStatus(channel);

        Assertions.assertEquals(status, result);
    }
}