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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ResultProcessorTest {
    Relationship successRelationship;
    Relationship failureRelationship;

    ResultProcessor resultProcessor;

    @BeforeEach
    public void setup() {
        successRelationship = new Relationship.Builder().build();
        failureRelationship = new Relationship.Builder().build();
        resultProcessor = new ResultProcessor(successRelationship, failureRelationship);
    }

    @Test
    public void testProcessResultFileSuccess() {
        ProcessSession processSession = mock(ProcessSession.class);
        ComponentLog componentLog = mock(ComponentLog.class);
        FlowFile flowFile = mock(FlowFile.class);
        Exception exception = null;
        String name = "basename";

        when(processSession.putAttribute(eq(flowFile), anyString(), anyString())).thenReturn(flowFile);

        resultProcessor.process(processSession, componentLog, flowFile, exception, name);
        verify(processSession).putAttribute(flowFile, CoreAttributes.FILENAME.key(), name);
        verify(processSession).putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MediaType.APPLICATION_XML_UTF_8.toString());
        verify(processSession).transfer(flowFile, successRelationship);
        verifyNoMoreInteractions(componentLog);
    }

    @Test
    public void testProcessResultFileFalure() {
        ProcessSession processSession = mock(ProcessSession.class);
        ComponentLog componentLog = mock(ComponentLog.class);
        FlowFile flowFile = mock(FlowFile.class);
        Exception exception = new Exception();
        String name = "name";

        when(processSession.putAttribute(eq(flowFile), anyString(), anyString())).thenReturn(flowFile);

        resultProcessor.process(processSession, componentLog, flowFile, exception, name);
        verify(processSession).putAttribute(flowFile, CoreAttributes.FILENAME.key(), name);
        verify(processSession).putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MediaType.APPLICATION_XML_UTF_8.toString());
        verify(processSession).transfer(flowFile, failureRelationship);
        verify(componentLog).error(eq(ResultProcessor.UNABLE_TO_PROCESS_DUE_TO), any(Object[].class));
    }
}
