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

package org.apache.nifi.minifi.nar;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NarAutoUnloaderTest {

    @Mock
    private NarAutoUnloaderTaskFactory narAutoUnloaderTaskFactory;

    @InjectMocks
    private NarAutoUnloader victim;

    @Test
    public void testTaskRunNotCalledMultipleTimes() throws Exception {
        NarAutoUnloaderTask task = mock(NarAutoUnloaderTask.class);

        when(narAutoUnloaderTaskFactory.createNarAutoUnloaderTask()).thenReturn(task);

        victim.start();
        victim.start();

        verify(task, times(1)).run();
    }

    @Test
    public void testTaskStopNotCalledIfNotStarted() throws Exception {
        NarAutoUnloaderTask task = mock(NarAutoUnloaderTask.class);

        victim.stop();

        verifyNoInteractions(task);
    }

    @Test
    public void testTaskStopCalledIfStartedButNotCalledMultipleTimes() throws Exception {
        NarAutoUnloaderTask task = mock(NarAutoUnloaderTask.class);

        when(narAutoUnloaderTaskFactory.createNarAutoUnloaderTask()).thenReturn(task);

        victim.start();
        victim.stop();
        victim.stop();

        verify(task, times(1)).stop();
    }
}