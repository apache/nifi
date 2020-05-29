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
package org.apache.nifi.web.controller;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.nifi.web.controller.ComponentMockUtil.getControllerServiceNode;
import static org.apache.nifi.web.controller.ComponentMockUtil.getChildProcessGroup;
import static org.apache.nifi.web.controller.ComponentMockUtil.getConnection;
import static org.apache.nifi.web.controller.ComponentMockUtil.getFunnel;
import static org.apache.nifi.web.controller.ComponentMockUtil.getLabel;
import static org.apache.nifi.web.controller.ComponentMockUtil.getParameter;
import static org.apache.nifi.web.controller.ComponentMockUtil.getParameterContext;
import static org.apache.nifi.web.controller.ComponentMockUtil.getPort;
import static org.apache.nifi.web.controller.ComponentMockUtil.getProcessorNode;
import static org.apache.nifi.web.controller.ComponentMockUtil.getPublicPort;
import static org.apache.nifi.web.controller.ComponentMockUtil.getRemoteProcessGroup;
import static org.apache.nifi.web.controller.ComponentMockUtil.getRootProcessGroup;
import static org.apache.nifi.web.controller.ComponentMockUtil.getSearchableProcessor;

public class ControllerSearchServiceRegressionTest extends AbstractControllerSearchIntegrationTest {
    @Test
    public void testTextOmniMatch() {
        // given
        final String omniMatch = "omniMatch";
        final ProcessGroup rootProcessGroup = getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "root_comments_omniMatch", true, false);

        final ProcessorNode processor1 = getProcessorNode(
            "proc1_id_omniMatch",
            "proc1_name_omniMatch",
            "proc1_comments_omniMatch",
            Optional.of("proc1_versionedId_omniMatch"),
            SchedulingStrategy.TIMER_DRIVEN,
            ExecutionNode.ALL,
            ScheduledState.STOPPED,
            ValidationStatus.VALID,
            Arrays.asList(
                new Relationship.Builder().autoTerminateDefault(false).name("proc1_rel1_name_omniMatch").description("no_find_omniMatch").build(),
                new Relationship.Builder().autoTerminateDefault(false).name("proc1_rel2_name_omniMatch").description("no_find_omniMatch").build()
            ),
            "proc1_type_omniMatch",
            getSearchableProcessor(Arrays.asList(
                new SearchResult.Builder().label("proc1_search1_label_omniMatch").match("proc1_search1_match_omniMatch").build(),
                new SearchResult.Builder().label("proc1_search2_label_omniMatch").match("proc1_search2_match_omniMatch").build()
            )),
            new HashMap<PropertyDescriptor, String>(){{
                put(new PropertyDescriptor.Builder().name("proc1_prop1_name_omniMatch").description("proc1_prop1_description_omniMatch").build(), "proc1_prop1_value_omniMatch");
                put(new PropertyDescriptor.Builder().name("proc1_prop2_name_omniMatch").description("proc1_prop2_description_omniMatch").build(), "proc1_prop2_value_omniMatch");
            }},
            AUTHORIZED
        );

        final ComponentSearchResultDTO proc1Result = getSimpleResultFromRoot("proc1_id_omniMatch", "proc1_name_omniMatch",
            "Id: proc1_id_omniMatch",
            "Version Control ID: proc1_versionedId_omniMatch",
            "Name: proc1_name_omniMatch",
            "Comments: proc1_comments_omniMatch",
            "Relationship: proc1_rel1_name_omniMatch",
            "Relationship: proc1_rel2_name_omniMatch",
            "Type: proc1_type_omniMatch",
            "Property name: proc1_prop1_name_omniMatch",
            "Property description: proc1_prop1_description_omniMatch",
            "Property value: proc1_prop1_name_omniMatch - proc1_prop1_value_omniMatch",
            "Property name: proc1_prop2_name_omniMatch",
            "Property description: proc1_prop2_description_omniMatch",
            "Property value: proc1_prop2_name_omniMatch - proc1_prop2_value_omniMatch",
            "proc1_search1_label_omniMatch: proc1_search1_match_omniMatch",
            "proc1_search2_label_omniMatch: proc1_search2_match_omniMatch"
        );

        final Funnel funnel1 = getFunnel(
            "funnel1_id_omniMatch",
            "funnel1_versionedId_omniMatch",
            "funnel1_comments_omniMatch",
            AUTHORIZED
        );

        final ComponentSearchResultDTO funnel1Result = getSimpleResultFromRoot("funnel1_id_omniMatch", null,
            "Id: funnel1_id_omniMatch",
            "Version Control ID: funnel1_versionedId_omniMatch"
        );

        final Connection connection1 = getConnection(
            "conn1_id_omniMatch",
            "conn1_name_omniMatch",
            Optional.of("conn1_versionedId_omniMatch"),
            Arrays.asList(
                new Relationship.Builder().autoTerminateDefault(false).name("conn1_rel1_name_omniMatch").description("no_find_omniMatch").build(),
                new Relationship.Builder().autoTerminateDefault(false).name("conn1_rel2_name_omniMatch").description("no_find_omniMatch").build()
            ),
            Arrays.asList(),
            1,
            "1 MB",
            1L,
            processor1,
            funnel1,
            AUTHORIZED
        );

        final ComponentSearchResultDTO connection1Result = getSimpleResultFromRoot("conn1_id_omniMatch", "conn1_name_omniMatch",
            "Id: conn1_id_omniMatch",
            "Version Control ID: conn1_versionedId_omniMatch",
            "Name: conn1_name_omniMatch",
            "Relationship: conn1_rel1_name_omniMatch",
            "Relationship: conn1_rel2_name_omniMatch",
            "Source id: proc1_id_omniMatch",
            "Source name: proc1_name_omniMatch",
            "Source comments: proc1_comments_omniMatch",
            "Destination id: funnel1_id_omniMatch",
            "Destination comments: funnel1_comments_omniMatch"
        );

        final Port inputPort1 = getPort(
            "inport1_id_omniMatch",
            "inport1_name_omniMatch",
            "inport1_versionedId_omniMatch",
            "inport1_comments_omniMatch",
            ScheduledState.STOPPED,
            true,
            AUTHORIZED
        );

        final ComponentSearchResultDTO inputPort1Result = getSimpleResultFromRoot("inport1_id_omniMatch", "inport1_name_omniMatch",
            "Id: inport1_id_omniMatch",
            "Version Control ID: inport1_versionedId_omniMatch",
            "Name: inport1_name_omniMatch",
            "Comments: inport1_comments_omniMatch"
        );

        final Port outpuPublicPort1 = getPublicPort(
            "outpublicport1_id_omniMatch",
            "outpublicport1_name_omniMatch",
            "outpublicport1_comments_omniMatch",
            "outpublicport1_versionedId_omniMatch",
            ScheduledState.STOPPED,
            true,
            AUTHORIZED,
            Arrays.asList("outpublicport1_userAccessControl1_omniMatch", "outpublicport1_userAccessControl2_omniMatch"),
            Arrays.asList("outpublicport1_groupAccessControl1_omniMatch", "outpublicport1_groupAccessControl2_omniMatch")
        );

        final ComponentSearchResultDTO outputPublicPort1Result = getSimpleResultFromRoot("outpublicport1_id_omniMatch", "outpublicport1_name_omniMatch",
            "Id: outpublicport1_id_omniMatch",
            "Version Control ID: outpublicport1_versionedId_omniMatch",
            "Name: outpublicport1_name_omniMatch",
            "Comments: outpublicport1_comments_omniMatch",
            "User access control: outpublicport1_userAccessControl1_omniMatch",
            "User access control: outpublicport1_userAccessControl2_omniMatch",
            "Group access control: outpublicport1_groupAccessControl1_omniMatch",
            "Group access control: outpublicport1_groupAccessControl2_omniMatch"
        );

        final Label label1 = getLabel("label1_id_omniMatch", "label1_value_omniMatch", true);

        final ComponentSearchResultDTO label1Result = getSimpleResultFromRoot("label1_id_omniMatch", "label1_value_omniMatch",
            "Id: label1_id_omniMatch",
            "Value: label1_value_omniMatch"
        );

        final ControllerServiceNode controllerServiceNode1 = getControllerServiceNode(
                "controllerServiceNode1_id_omniMatch",
                "controllerServiceNode1_name_omniMatch",
                "controllerServiceNode1_comments_omniMatch",
                new HashMap<PropertyDescriptor, String>(){{
                    put(new PropertyDescriptor.Builder()
                                .name("controllerServiceNode1_prop1_name_omniMatch")
                                .description("controllerServiceNode1_prop1_description_omniMatch")
                                .build(),
                            "controllerServiceNode1_prop1_value_omniMatch");
                    put(new PropertyDescriptor.Builder()
                                .name("controllerServiceNode1_prop2_name_omniMatch")
                                .description("controllerServiceNode1_prop2_description_omniMatch")
                                .build(),
                            "controllerServiceNode1_prop2_value_omniMatch");
                }},
                "controllerServiceNode1_versioned_id_omniMatch",
                AUTHORIZED
        );

        final ComponentSearchResultDTO controllerServiceNode1Result = getSimpleResultFromRoot("controllerServiceNode1_id_omniMatch", "controllerServiceNode1_name_omniMatch",
                "Id: controllerServiceNode1_id_omniMatch",
                "Name: controllerServiceNode1_name_omniMatch",
                "Comments: controllerServiceNode1_comments_omniMatch",
                "Version Control ID: controllerServiceNode1_versioned_id_omniMatch",
                "Property name: controllerServiceNode1_prop1_name_omniMatch",
                "Property description: controllerServiceNode1_prop1_description_omniMatch",
                "Property value: controllerServiceNode1_prop1_name_omniMatch - controllerServiceNode1_prop1_value_omniMatch",
                "Property name: controllerServiceNode1_prop2_name_omniMatch",
                "Property description: controllerServiceNode1_prop2_description_omniMatch",
                "Property value: controllerServiceNode1_prop2_name_omniMatch - controllerServiceNode1_prop2_value_omniMatch"
        );

        final ProcessGroup processGroup1 = getChildProcessGroup(
            "processgroup1_id_omniMatch",
            "processgroup1_name_omniMatch",
            "processgroup1_comments_omniMatch",
            "processgroup1_versionedId_omniMatch",
            rootProcessGroup,
            AUTHORIZED,
            UNDER_VERSION_CONTROL
        );

        final ComponentVariableRegistry variableRegistry = Mockito.mock(ComponentVariableRegistry.class);

        Mockito.when(processGroup1.getVariableRegistry()).thenReturn(variableRegistry);
        Mockito.when(variableRegistry.getVariableMap()).thenReturn(new HashMap<VariableDescriptor, String>(){{
            put(new VariableDescriptor.Builder("processgroup1_variable1_key_omniMatch").description("no_find_omniMatch").build(), "processgroup1_variable1_value_omniMatch");
            put(new VariableDescriptor.Builder("processgroup1_variable2_key_omniMatch").description("no_find_omniMatch").build(), "processgroup1_variable2_value_omniMatch");
        }});

        final ComponentSearchResultDTO processGroup1Result = getSimpleResultFromRoot("processgroup1_id_omniMatch", "processgroup1_name_omniMatch",
            "Id: processgroup1_id_omniMatch",
            "Version Control ID: processgroup1_versionedId_omniMatch",
            "Name: processgroup1_name_omniMatch",
            "Comments: processgroup1_comments_omniMatch",
            "Variable Name: processgroup1_variable1_key_omniMatch",
            "Variable Value: processgroup1_variable1_value_omniMatch",
            "Variable Name: processgroup1_variable2_key_omniMatch",
            "Variable Value: processgroup1_variable2_value_omniMatch"
        );

        final ComponentSearchResultDTO rootProcessGroupResult = getSimpleResult(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME,
                ROOT_PROCESSOR_GROUP_ID,
                null,
                null,
                "Comments: root_comments_omniMatch"
        );

        final RemoteProcessGroup remoteProcessGroup1 = getRemoteProcessGroup(
            "remoteprocessgroup1_id_omniMatch",
            "remoteprocessgroup1_name_omniMatch",
            Optional.of("remoteprocessgroup1_versionedId_omniMatch"),
            "remoteprocessgroup1_comments_omniMatch",
            "remoteprocessgroup1_targetUris_omniMatch",
            false,
            AUTHORIZED
        );

        final ComponentSearchResultDTO remoteProcessGroup1Result = getSimpleResultFromRoot("remoteprocessgroup1_id_omniMatch", "remoteprocessgroup1_name_omniMatch",
            "Id: remoteprocessgroup1_id_omniMatch",
            "Version Control ID: remoteprocessgroup1_versionedId_omniMatch",
            "Name: remoteprocessgroup1_name_omniMatch",
            "Comments: remoteprocessgroup1_comments_omniMatch",
            "URLs: remoteprocessgroup1_targetUris_omniMatch"
        );

        final ParameterContext parameterContext1 = getParameterContext(
            "parametercontext1_id_omniMatch",
            "parametercontext1_name_omniMatch",
            "parametercontext1_description_omniMatch",
            givenParameters(
                getParameter("parametercontext1_parameter1_name_omniMatch", "parametercontext1_parameter1_value_omniMatch", false, "parametercontext1_parameter1_description_omniMatch"),
                getParameter("parametercontext1_parameter2_name_omniMatch", "sensitive_no_find_omniMatch", true, "parametercontext1_parameter2_description_omniMatch")
            ),
            AUTHORIZED
        );

        final ComponentSearchResultDTO parameterContext1Result = getSimpleResult("parametercontext1_id_omniMatch", "parametercontext1_name_omniMatch", null, null, null,
            "Id: parametercontext1_id_omniMatch",
            "Name: parametercontext1_name_omniMatch",
            "Description: parametercontext1_description_omniMatch"
        );

        final Collection<ComponentSearchResultDTO> parameterResults1 = Arrays.asList(
            getSimpleResult(
                "parametercontext1_parameter1_name_omniMatch",
                "parametercontext1_parameter1_name_omniMatch",
                null,
                "parametercontext1_id_omniMatch",
                "parametercontext1_name_omniMatch",
                "Name: parametercontext1_parameter1_name_omniMatch",
                "Description: parametercontext1_parameter1_description_omniMatch",
                "Value: parametercontext1_parameter1_value_omniMatch"
            ),
            getSimpleResult(
                "parametercontext1_parameter2_name_omniMatch",
                "parametercontext1_parameter2_name_omniMatch",
                null,
                "parametercontext1_id_omniMatch",
                "parametercontext1_name_omniMatch",
                "Name: parametercontext1_parameter2_name_omniMatch",
                "Description: parametercontext1_parameter2_description_omniMatch"
            )
        );

        givenParameterContext(parameterContext1);
        givenProcessGroup(rootProcessGroup)
            .withProcessor(processor1)
            .withFunnel(funnel1)
            .withConnection(connection1)
            .withInputPort(inputPort1)
            .withOutputPort(outpuPublicPort1)
            .withLabel(label1)
            .withControllerServiceNode(controllerServiceNode1)
            .withChild(processGroup1)
            .withRemoteProcessGroup(remoteProcessGroup1);

        // when
        whenExecuteSearch(omniMatch);

        // then
        thenResultConsists()
            .ofProcessor(proc1Result)
            .ofFunnel(funnel1Result)
            .ofConnection(connection1Result)
            .ofInputPort(inputPort1Result)
            .ofOutputPort(outputPublicPort1Result)
            .ofLabel(label1Result)
            .ofControllerServiceNode(controllerServiceNode1Result)
            .ofProcessGroup(processGroup1Result)
            .ofProcessGroup(rootProcessGroupResult)
            .ofRemoteProcessGroup(remoteProcessGroup1Result)
            .ofParameterContext(parameterContext1Result)
            .ofParameter(parameterResults1)
            .validate(results);
    }

    @Test
    public void testSearchInRootLevelAllAuthorizedNoVersionControl() {
        // given
        givenBasicStructure();
        getProcessGroupSetup(ROOT_PROCESSOR_GROUP_ID).withProcessor(getProcessorNode("foobarId", "foobar", AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResultFromRoot("foobarId", "foobar", "Id: foobarId", "Name: foobar"))
                .validate(results);
    }

    @Test
    public void testSearchInThirdLevelAllAuthorizedNoVersionControl() {
        // given
        givenBasicStructure();
        getProcessGroupSetup("thirdLevelAId").withProcessor(getProcessorNode("foobarId", "foobar", AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("foobarId", "foobar", "thirdLevelAId", "thirdLevelAId", "thirdLevelA", "Id: foobarId", "Name: foobar"))
                .validate(results);
    }

    @Test
    public void testSearchInThirdLevelParentNotAuthorizedNoVersionControl() {
        // given
        givenBasicStructure();
        givenThirdLevelIsNotAuthorized();
        getProcessGroupSetup("thirdLevelAId").withProcessor(getProcessorNode("foobarId", "foobar", AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofProcessor(getSimpleResult("foobarId", "foobar", "thirdLevelAId", "thirdLevelAId", null, "Id: foobarId", "Name: foobar"))
                .validate(results);
    }

    @Test
    public void testSearchInThirdLevelParentNotAuthorizedWithVersionControl() {
        // given
        givenBasicStructure();
        givenThirdLevelIsNotAuthorized();
        givenProcessorGroupIsUnderVersionControl("firstLevelAId");
        getProcessGroupSetup("thirdLevelAId").withProcessor(getProcessorNode("foobarId", "foobar", AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofProcessor(getVersionedResult("foobarId", "foobar", "thirdLevelAId", "thirdLevelAId", null, "firstLevelAId", "firstLevelA", "Id: foobarId", "Name: foobar"))
                .validate(results);
    }

    @Test
    public void testSearchInThirdLevelParentNotAuthorizedWithVersionControlInTheGroup() {
        // given
        givenBasicStructure();
        givenThirdLevelIsNotAuthorized();
        givenProcessorGroupIsUnderVersionControl("thirdLevelAId");
        getProcessGroupSetup("thirdLevelAId").withProcessor(getProcessorNode("foobarId", "foobar", AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofProcessor(getVersionedResult("foobarId", "foobar", "thirdLevelAId", "thirdLevelAId", null, "thirdLevelAId", null, "Id: foobarId", "Name: foobar"))
                .validate(results);
    }

    @Test
    public void testSearchParameterContext() {
        // given
        givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "", AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        final Map<ParameterDescriptor, Parameter> fooParameters = givenParameters(
                getParameter("foo_1", "foo_1_value", false, "Description for foo_1"));

        final Map<ParameterDescriptor, Parameter> barParameters = givenParameters(
                getParameter("bar_1", "bar_1_value", false, "Description for bar_1"),
                getParameter("bar_2", "bar_2_value", false, "Description for bar_2"));

        givenParameterContext(getParameterContext("fooId", "foo", "description for parameter context foo", fooParameters, AUTHORIZED));
        givenParameterContext(getParameterContext("barId", "bar", "description for parameter context bar", barParameters, AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofParameterContext(getSimpleResult("fooId", "foo", null, null, null, "Id: fooId", "Name: foo", "Description: description for parameter context foo"))
                .ofParameter(getSimpleResult("foo_1", "foo_1", null, "fooId", "foo", "Name: foo_1", "Value: foo_1_value", "Description: Description for foo_1"))
                .validate(results);
    }

    @Test
    public void testSearchParameterContextNotAuthorized() {
        // given
        givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "", AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        final Map<ParameterDescriptor, Parameter> fooParameters = givenParameters(
                getParameter("foo_1", "foo_1_value", false, "Description for foo_1"));

        final Map<ParameterDescriptor, Parameter> barParameters = givenParameters(
                getParameter("bar_1", "bar_1_value", false, "Description for bar_1"),
                getParameter("bar_2", "bar_2_value", false, "Description for bar_2"));

        givenParameterContext(getParameterContext("fooId", "foo", "description for parameter context foo", fooParameters, NOT_AUTHORIZED));
        givenParameterContext(getParameterContext("barId", "bar", "description for parameter context bar", barParameters, AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultIsEmpty();
    }

    @Test
    public void testSearchLabels() {
        // given
        givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "", AUTHORIZED, NOT_UNDER_VERSION_CONTROL))
                .withLabel(getLabel("foo", "Value for label foo", AUTHORIZED))
                .withLabel(getLabel("bar", "Value for label bar, but FOO is in here too", NOT_AUTHORIZED));

        // when
        whenExecuteSearch("foo");

        // then
        thenResultConsists()
                .ofLabel(getSimpleResult("foo", "Value for label foo", ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "Id: foo", "Value: Value for label foo"))
                .validate(results);
    }

    @Test
    public void testSearchControllerServices() {
        // given
        final String name = "controllerServiceName";
        final String id = name + "Id";
        final Map<PropertyDescriptor, String> rawProperties = new HashMap<PropertyDescriptor, String>(){{
            put(new PropertyDescriptor.Builder().name("prop1-name").displayName("prop1-displayname").description("prop1 description").defaultValue("prop1-default").build(), "prop1-value");
            put(new PropertyDescriptor.Builder().name("prop2-name").displayName("prop2-displayname").description("prop2 description").defaultValue("prop2-default").build(), null);
        }};


        givenRootProcessGroup()
                .withControllerServiceNode(getControllerServiceNode(id, name, "foo comment", rawProperties, AUTHORIZED));

        // when - search for name
        whenExecuteSearch("controllerserv");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Name: controllerServiceName", "Id: controllerServiceNameId"))
                .validate(results);


        // when - search for comments
        whenExecuteSearch("foo comment");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Comments: foo comment"))
                .validate(results);


        // when - search for properties
        whenExecuteSearch("prop1-name");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Property name: prop1-name"))
                .validate(results);


       // when - property default value
        whenExecuteSearch("prop2-def");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Property value: prop2-name - prop2-default"))
                .validate(results);


        // when - property description
        whenExecuteSearch("desc");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Property description: prop1 description", "Property description: prop2 description"))
                .validate(results);


        // when - by specified value
        whenExecuteSearch("prop1-value");

        // then
        thenResultConsists()
                .ofControllerServiceNode(getSimpleResultFromRoot(id, name, "Property value: prop1-name - prop1-value"))
                .validate(results);


        // when - search finding no match
        whenExecuteSearch("ZZZZZZZZZYYYYYY");

        // then
        thenResultIsEmpty();


        // when - properties are filtered out
        whenExecuteSearch("properties:exclude prop1");

        // then
        thenResultIsEmpty();
    }


    // Helper methods

    private void givenBasicStructure() {
        givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "", ROOT_PROCESSOR_GROUP_ID, AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        givenProcessGroup(getChildProcessGroup("firstLevelAId", "firstLevelA", "", "firstLevelAId", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("firstLevelBId", "firstLevelB", "", "firstLevelBId", getProcessGroup(ROOT_PROCESSOR_GROUP_ID), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        givenProcessGroup(getChildProcessGroup("secondLevelAId", "secondLevelA", "", "secondLevelAId", getProcessGroup("firstLevelAId"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("secondLevelBId", "secondLevelB", "", "secondLevelBId", getProcessGroup("firstLevelBId"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));

        givenProcessGroup(getChildProcessGroup("thirdLevelAId", "thirdLevelA", "", "thirdLevelAId", getProcessGroup("secondLevelAId"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
        givenProcessGroup(getChildProcessGroup("thirdLevelBId", "thirdLevelB", "", "thirdLevelBId", getProcessGroup("secondLevelBId"), AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
    }

    private void givenThirdLevelIsNotAuthorized() {
        Mockito.when(getProcessGroup("thirdLevelAId").isAuthorized(Mockito.any(Authorizer.class), Mockito.any(RequestAction.class), Mockito.any(NiFiUser.class))).thenReturn(false);
        Mockito.when(getProcessGroup("thirdLevelBId").isAuthorized(Mockito.any(Authorizer.class), Mockito.any(RequestAction.class), Mockito.any(NiFiUser.class))).thenReturn(false);
    }

    private void givenProcessorGroupIsUnderVersionControl(final String processGroupId) {
        Mockito.when(getProcessGroup(processGroupId).getVersionControlInformation()).thenReturn(Mockito.mock(VersionControlInformation.class));
    }
}
