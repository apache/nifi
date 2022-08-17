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

package org.apache.nifi.processors.stateless.retrieval;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.stateless.ExecuteStateless;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestCachingDataflowProvider {

    @Test
    public void testDelegatesEvenAfterFetch() throws IOException {
        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();

        final DataflowProvider mockedRetrieval = mock(DataflowProvider.class);
        when(mockedRetrieval.retrieveDataflowContents(any(ProcessContext.class))).thenReturn(snapshot);

        final CachingDataflowProvider retrieval = new CachingDataflowProvider("1234", mock(ComponentLog.class), mockedRetrieval);

        final ProcessContext context = mock(ProcessContext.class);
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn("target/testFetchFirst/cache");
        when(context.getProperty(ExecuteStateless.WORKING_DIRECTORY)).thenReturn(propertyValue);

        final File cacheFile = retrieval.getFlowCacheFile(context, "1234");

        for (int i=0; i < 2; i++) {
            retrieval.retrieveDataflowContents(context);
            assertTrue(cacheFile.exists());
        }

        verify(mockedRetrieval, times(2)).retrieveDataflowContents(context);
    }

    @Test
    public void testThrowsIfDelegateThrowsAndNoCache() throws IOException {
        final DataflowProvider mockedRetrieval = mock(DataflowProvider.class);
        when(mockedRetrieval.retrieveDataflowContents(any(ProcessContext.class))).thenThrow(new IOException("Intentional exception for testing purposes"));

        final CachingDataflowProvider retrieval = new CachingDataflowProvider("1234", mock(ComponentLog.class), mockedRetrieval);

        final ProcessContext context = mock(ProcessContext.class);
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn("target/testThrowsIfDelegateThrowsAndNoCache/cache");
        when(context.getProperty(ExecuteStateless.WORKING_DIRECTORY)).thenReturn(propertyValue);

        final File cacheFile = retrieval.getFlowCacheFile(context, "1234");

        assertThrows(IOException.class, () -> retrieval.retrieveDataflowContents(context));
        assertFalse(cacheFile.exists());
    }

    @Test
    public void testFetchesCachedFlowOnException() throws IOException {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName("Testable Group");
        final VersionedFlowSnapshot snapshot = new VersionedFlowSnapshot();
        snapshot.setFlowContents(group);

        final DataflowProvider mockedRetrieval = mock(DataflowProvider.class);
        doAnswer(new Answer<VersionedFlowSnapshot>() {
            private int count;

            @Override
            public VersionedFlowSnapshot answer(final InvocationOnMock invocation) throws Throwable {
                if (count++ == 0) {
                    return snapshot;
                }

                throw new IOException("Intentional failure for testing purposes");
            }
        }).when(mockedRetrieval).retrieveDataflowContents(any(ProcessContext.class));

        final CachingDataflowProvider retrieval = new CachingDataflowProvider("1234", mock(ComponentLog.class), mockedRetrieval);

        final ProcessContext context = mock(ProcessContext.class);
        final PropertyValue propertyValue = mock(PropertyValue.class);
        when(propertyValue.getValue()).thenReturn("target/testFetchesCachedFlowOnException/cache");
        when(context.getProperty(ExecuteStateless.WORKING_DIRECTORY)).thenReturn(propertyValue);

        final File cacheFile = retrieval.getFlowCacheFile(context, "1234");

        VersionedFlowSnapshot retrieved = null;
        for (int i=0; i < 2; i++) {
            retrieved = retrieval.retrieveDataflowContents(context);
            assertNotNull(retrieved);

            assertTrue(cacheFile.exists());
        }

        verify(mockedRetrieval, times(2)).retrieveDataflowContents(context);
        assertEquals(group.getName(), retrieved.getFlowContents().getName());
    }
}
