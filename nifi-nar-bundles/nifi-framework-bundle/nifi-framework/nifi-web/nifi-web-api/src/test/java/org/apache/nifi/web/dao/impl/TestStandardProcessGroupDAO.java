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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ParameterContextReferenceEntity;
import org.junit.Test;

public class TestStandardProcessGroupDAO {
    private static final String PARENT_PROCESS_GROUP_ID = "parentId";
    private static final String PROCESS_GROUP_ID = "pgId";
    private static final String PROCESS_GROUP_NAME = "pgName";
    private static final String PROCESS_GROUP_COMMENTS = "This is a comment";
    private static final String PARAMETER_CONTEXT_ID = "paramContext";

    @Test
    public void testCreateProcessGroup() {
        final StandardProcessGroupDAO dao = new StandardProcessGroupDAO();
        final FlowController flowController = mock(FlowController.class);
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowController.getFlowManager()).thenReturn(flowManager);
        dao.setFlowController(flowController);

        final ProcessGroup parentProcessGroup = mock(ProcessGroup.class);
        when(flowManager.getGroup(PARENT_PROCESS_GROUP_ID)).thenReturn(parentProcessGroup);

        final ParameterContextReferenceEntity parameterContextReferenceEntity = mock(ParameterContextReferenceEntity.class);
        final ParameterContextManager parameterContextManager = mock(ParameterContextManager.class);
        final ParameterContext parameterContext = mock(ParameterContext.class);

        final ProcessGroupDTO processGroupDTO = mock(ProcessGroupDTO.class);
        when(processGroupDTO.getId()).thenReturn(PROCESS_GROUP_ID);
        when(processGroupDTO.getName()).thenReturn(PROCESS_GROUP_NAME);
        when(processGroupDTO.getComments()).thenReturn(PROCESS_GROUP_COMMENTS);
        final PositionDTO position = mock(PositionDTO.class);
        when(processGroupDTO.getPosition()).thenReturn(position);

        when(flowManager.getParameterContextManager()).thenReturn(parameterContextManager);
        when(parameterContextManager.getParameterContext(PARAMETER_CONTEXT_ID)).thenReturn(parameterContext);
        when(processGroupDTO.getParameterContext()).thenReturn(parameterContextReferenceEntity);
        when(parameterContextReferenceEntity.getId()).thenReturn(PARAMETER_CONTEXT_ID);

        final ProcessGroup childProcessGroup = mock(ProcessGroup.class);
        when(flowManager.createProcessGroup(PROCESS_GROUP_ID)).thenReturn(childProcessGroup);

        dao.createProcessGroup(PARENT_PROCESS_GROUP_ID, processGroupDTO);

        verify(childProcessGroup).setName(PROCESS_GROUP_NAME);
        verify(childProcessGroup).setComments(PROCESS_GROUP_COMMENTS);
        verify(childProcessGroup).setPosition(any(Position.class));
        verify(childProcessGroup).setParameterContext(parameterContext);
        verify(childProcessGroup).setParent(parentProcessGroup);
        verify(parentProcessGroup).addProcessGroup(childProcessGroup);
    }
}
