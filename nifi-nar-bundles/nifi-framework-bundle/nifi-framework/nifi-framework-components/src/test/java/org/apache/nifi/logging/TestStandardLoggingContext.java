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
package org.apache.nifi.logging;

import org.apache.nifi.groups.ProcessGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardLoggingContext {
    private static final String LOG_FILE_SUFFIX = "myGroup";

    @Mock
    private GroupedComponent processor;

    @Mock
    private ProcessGroup processGroup;

    @Test
    void testNullComponent_ShouldReturnOptionalEmpty() {
        LoggingContext context = new StandardLoggingContext(null);

        assertTrue(context.getLogFileSuffix().isEmpty());
    }

    @Test
    void testComponentWithProcessGroups_WithoutPerProcessGroupLogging_ShouldReturnOptionalEmpty() {
        //component with pg with no setting returns optional empty
        LoggingContext context = new StandardLoggingContext(processor);
        when(processor.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.getLogFileSuffix()).thenReturn(null, null);
        when(processGroup.isRootGroup()).thenReturn(Boolean.FALSE, Boolean.TRUE);
        when(processGroup.getParent()).thenReturn(processGroup);

        assertTrue(context.getLogFileSuffix().isEmpty());
    }

    @Test
    void testComponentWithProcessGroup_WithPerProcessGroupLogging_ShouldReturnLogFileSuffix() {
        LoggingContext context = new StandardLoggingContext(processor);
        when(processor.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.getLogFileSuffix()).thenReturn(LOG_FILE_SUFFIX);

        assertEquals(LOG_FILE_SUFFIX, context.getLogFileSuffix().orElse(null));
    }

    @Test
    void testComponentWithProcessGroups_WithPerProcessGroupLoggingSetOnParent_ShouldReturnLogFileSuffix() {
        LoggingContext context = new StandardLoggingContext(processor);
        when(processor.getProcessGroup()).thenReturn(processGroup);
        when(processGroup.isRootGroup()).thenReturn(Boolean.FALSE);
        when(processGroup.getParent()).thenReturn(processGroup);
        when(processGroup.getLogFileSuffix()).thenReturn(null, LOG_FILE_SUFFIX);

        assertEquals(LOG_FILE_SUFFIX, context.getLogFileSuffix().orElse(null));
    }
}