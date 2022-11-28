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

package org.apache.nifi.web.dao.impl;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TestStandardProcessGroupDAO {
    private static final String PARENT_PROCESS_GROUP_ID = "parentId";
    private static final String PROCESS_GROUP_ID = "pgId";
    private static final String PROCESS_GROUP_NAME = "pgName";
    private static final String PROCESS_GROUP_COMMENTS = "This is a comment";
    private static final String PARAMETER_CONTEXT_ID = "parameterContextId";

    private StandardProcessGroupDAO testSubject;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private FlowController flowController;

    @Mock
    private ProcessGroup parentProcessGroup;

    @Mock
    private ParameterContext parameterContext;

    @BeforeEach
    void setUp() {
        testSubject = new StandardProcessGroupDAO();
        testSubject.setFlowController(flowController);

        when(flowController
                .getFlowManager()
                .getGroup(PARENT_PROCESS_GROUP_ID)
        ).thenReturn(parentProcessGroup);

        when(flowController
                .getFlowManager()
                .getParameterContextManager()
                .getParameterContext(PARAMETER_CONTEXT_ID)
        ).thenReturn(parameterContext);
    }

    @Test
    public void testCreateProcessGroup() {
        //GIVEN
        ParameterContextReferenceEntity parameterContextReferenceEntity = new ParameterContextReferenceEntity();
        parameterContextReferenceEntity.setId(PARAMETER_CONTEXT_ID);

        ProcessGroupDTO processGroupDTO = new ProcessGroupDTO();
        processGroupDTO.setId(PROCESS_GROUP_ID);
        processGroupDTO.setName(PROCESS_GROUP_NAME);
        processGroupDTO.setComments(PROCESS_GROUP_COMMENTS);
        processGroupDTO.setPosition(new PositionDTO(10.0, 20.0));
        processGroupDTO.setParameterContext(parameterContextReferenceEntity);

        //WHEN
        ProcessGroup createdProcessGroup = testSubject.createProcessGroup(PARENT_PROCESS_GROUP_ID, processGroupDTO);

        //THEN
        verify(createdProcessGroup).setParent(parentProcessGroup);
        verify(createdProcessGroup).setParameterContext(parameterContext);
        verify(createdProcessGroup).setName(PROCESS_GROUP_NAME);
        verify(createdProcessGroup).setComments(PROCESS_GROUP_COMMENTS);
        verify(createdProcessGroup).setPosition(any(Position.class));

        verify(parentProcessGroup).addProcessGroup(createdProcessGroup);
    }
}