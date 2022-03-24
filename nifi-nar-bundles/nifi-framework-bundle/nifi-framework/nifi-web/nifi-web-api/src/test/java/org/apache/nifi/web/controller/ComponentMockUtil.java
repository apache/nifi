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
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.mockito.AdditionalMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ComponentMockUtil {

    // Process group

    public static ProcessGroup getRootProcessGroup(
            final String id,
            final String name,
            final String comments,
            final boolean isAuthorized,
            final boolean isUnderVersionControl) {
        return getProcessGroup(id, name, comments, Optional.empty(), Optional.empty(), isAuthorized, isUnderVersionControl);
    }

    public static ProcessGroup getRootProcessGroup(
            final String id,
            final String name,
            final String comments,
            final String versionedId,
            final boolean isAuthorized,
            final boolean isUnderVersionControl) {
        return getProcessGroup(id, name, comments, Optional.of(versionedId), Optional.empty(), isAuthorized, isUnderVersionControl);
    }

    public static ProcessGroup getChildProcessGroup(
            final String id,
            final String name,
            final String comments,
            final ProcessGroup parent,
            final boolean isAuthorized,
            final boolean isUnderVersionControl) {
        return getProcessGroup(id, name, comments, Optional.empty(), Optional.of(parent), isAuthorized, isUnderVersionControl);
    }

    public static ProcessGroup getChildProcessGroup(
            final String id,
            final String name,
            final String comments,
            final String versionedId,
            final ProcessGroup parent,
            final boolean isAuthorized,
            final boolean isUnderVersionControl) {
        return getProcessGroup(id, name, comments, Optional.of(versionedId), Optional.of(parent), isAuthorized, isUnderVersionControl);
    }

    private static ProcessGroup getProcessGroup(
            final String id,
            final String name,
            final String comments,
            final Optional<String> versionedId,
            final Optional<ProcessGroup> parent,
            final boolean isAuthorized,
            final boolean isUnderVersionControl) {
        final ProcessGroup result = Mockito.mock(ProcessGroup.class);
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getComments()).thenReturn(comments);
        Mockito.when(result.getVersionedComponentId()).thenReturn(versionedId);

        if (isUnderVersionControl) {
            VersionControlInformation versionControlInformation = Mockito.mock(VersionControlInformation.class);
            Mockito.when(result.getVersionControlInformation()).thenReturn(versionControlInformation);
        }

        if (parent.isPresent()) {
            Mockito.when(result.isRootGroup()).thenReturn(false);
            Mockito.when(result.getParent()).thenReturn(parent.get());
        } else {
            Mockito.when(result.isRootGroup()).thenReturn(true);
            Mockito.when(result.getParent()).thenReturn(null);
        }

        setAuthorized(result, isAuthorized);
        return result;
    }

    // Processor

    public static <T extends Searchable & Processor> ProcessorNode getSearchableProcessorNode(
            final String id,
            final String name,
            final String comments,
            final Class<T> processorType,
            final Collection<SearchResult> searchResults,
            final boolean isAuthorized) {
       final T processor = Mockito.mock(processorType);
       Mockito.when(processor.search(Mockito.any(SearchContext.class))).thenReturn(searchResults);

       return getProcessorNode(id, name, comments, Optional.empty(), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING,
               ValidationStatus.VALID, new HashSet<>(), "Searchable", processor, new HashMap<>(), isAuthorized);
    }

    public static ProcessorNode getProcessorNode(final String id, final String name, final boolean isAuthorized) {
        return getProcessorNode(id, name, "", Optional.empty(), SchedulingStrategy.TIMER_DRIVEN, ExecutionNode.ALL, ScheduledState.RUNNING,
                ValidationStatus.VALID, getBasicRelationships(), "dummyProcessor", Mockito.mock(DummyProcessor.class), new HashMap<>(), isAuthorized);
    }

    public static ProcessorNode getProcessorNode(
            final String id,
            final String name,
            final SchedulingStrategy schedulingStrategy,
            final ExecutionNode executionNode,
            final ScheduledState scheduledState,
            final ValidationStatus validationStatus,
            final boolean isAuthorized) {
        return getProcessorNode(id, name, "", Optional.empty(), schedulingStrategy, executionNode, scheduledState, validationStatus,
                new HashSet<>(), "Processor", Mockito.mock(Processor.class), new HashMap<>(), isAuthorized);
    }

    public static ProcessorNode getProcessorNode(
            final String id,
            final String name,
            final String comments,
            final Optional<String> versionedId,
            final SchedulingStrategy schedulingStrategy,
            final ExecutionNode executionNode,
            final ScheduledState scheduledState,
            final ValidationStatus validationStatus,
            final Collection<Relationship> relationships,
            final String componentType,
            final Processor processor,
            final Map<PropertyDescriptor, String> rawProperties,
            final boolean isAuthorized) {

        final ProcessorNode result = Mockito.mock(ProcessorNode.class);
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getComments()).thenReturn(comments);
        Mockito.when(result.getVersionedComponentId()).thenReturn(versionedId);
        Mockito.when(result.getSchedulingStrategy()).thenReturn(schedulingStrategy);
        Mockito.when(result.getExecutionNode()).thenReturn(executionNode);
        Mockito.when(result.getScheduledState()).thenReturn(scheduledState);
        Mockito.when(result.getValidationStatus()).thenReturn(validationStatus);
        Mockito.when(result.getRelationships()).thenReturn(relationships);
        Mockito.when(result.getComponentType()).thenReturn(componentType);
        Mockito.when(result.getProcessor()).thenReturn(processor);
        Mockito.when(result.getRawPropertyValues()).thenReturn(rawProperties);

        setAuthorized(result, isAuthorized);
        return result;
    }

    public static Processor getSearchableProcessor(List<SearchResult> searchResults) {
        Processor searchableProcessor = Mockito.mock(Processor.class, Mockito.withSettings().extraInterfaces(Searchable.class));

        Mockito.when(((Searchable)searchableProcessor).search(Mockito.any(SearchContext.class))).thenReturn(searchResults);

        return searchableProcessor;
    }

    // Connection

    public static Connection getConnection(
            final String id,
            final String name,
            final Collection<Relationship> relationships,
            final Connectable source,
            final Connectable destination,
            final boolean isAuthorized) {
        return getConnection(id, name, Optional.empty(), relationships, new ArrayList<>(), 0, "0", 0L, source, destination, isAuthorized);
    }

    public static Connection getConnection(
            final String id,
            final String name,
            final Optional<String> versionedId,
            final Collection<Relationship> relationships,
            final List<FlowFilePrioritizer> flowFilePrioritizers,
            final int flowFileExpirationInMs,
            final String backPressureDataSize,
            final long backPressureCount,
            final Connectable source,
            final Connectable destination,
            final boolean isAuthorized) {
        final Connection result = Mockito.mock(Connection.class);
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);

        Mockito.when(flowFileQueue.getPriorities()).thenReturn(flowFilePrioritizers);
        Mockito.when(flowFileQueue.getFlowFileExpiration()).thenReturn(String.valueOf(flowFileExpirationInMs));
        Mockito.when(flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS)).thenReturn(flowFileExpirationInMs);
        Mockito.when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn(backPressureDataSize);
        Mockito.when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(backPressureCount);

        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getVersionedComponentId()).thenReturn(versionedId);
        Mockito.when(result.getRelationships()).thenReturn(relationships);
        Mockito.when(result.getFlowFileQueue()).thenReturn(flowFileQueue);
        Mockito.when(result.getSource()).thenReturn(source);
        Mockito.when(result.getDestination()).thenReturn(destination);

        setAuthorized(result, isAuthorized);
        return result;
    }

    // Remote process group

    public static RemoteProcessGroup getRemoteProcessGroup(
            final String id,
            final String name,
            final Optional<String> versionedId,
            final String comments,
            final String targetUris,
            final boolean isTransmitting,
            final boolean isAuthorized) {
        final RemoteProcessGroup result = Mockito.mock(RemoteProcessGroup.class);
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getComments()).thenReturn(comments);
        Mockito.when(result.getVersionedComponentId()).thenReturn(versionedId);
        Mockito.when(result.getTargetUris()).thenReturn(targetUris);
        Mockito.when(result.isTransmitting()).thenReturn(isTransmitting);
        setAuthorized(result, isAuthorized);

        return result;
    }

    // Port

    public static Port getPort(
            final String id,
            final String name,
            final String comments,
            final ScheduledState scheduledState,
            final boolean isValid,
            final boolean isAuthorized) {
        return getPort(Port.class, id, name, Optional.empty(), comments, scheduledState, isValid, isAuthorized);
    }

    public static Port getPort(
            final String id,
            final String name,
            final String versionedId,
            final String comments,
            final ScheduledState scheduledState,
            final boolean isValid,
            final boolean isAuthorized) {
        return getPort(Port.class, id, name, Optional.of(versionedId), comments, scheduledState, isValid, isAuthorized);
    }

    private static <T extends Port> T getPort(
            final Class<T> type,
            final String id,
            final String name,
            final Optional<String> versionedId,
            final String comments,
            final ScheduledState scheduledState,
            final boolean isValid,
            final boolean isAuthorized) {
        final T result = Mockito.mock(type);
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getComments()).thenReturn(comments);
        Mockito.when(result.getVersionedComponentId()).thenReturn(versionedId);
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getScheduledState()).thenReturn(scheduledState);
        Mockito.when(result.isValid()).thenReturn(isValid);
        setAuthorized(result, isAuthorized);
        return result;
    }

    public static Port getPublicPort(
            final String id,
            final String name,
            final String comments,
            final ScheduledState scheduledState,
            final boolean isValid,
            final boolean isAuthorized,
            final List<String> userAccessControl,
            final List<String> groupAccessControl) {
        final PublicPort result = getPort(PublicPort.class, id, name, Optional.empty(), comments, scheduledState, isValid, isAuthorized);
        Mockito.when(result.getUserAccessControl()).thenReturn(new HashSet<>(userAccessControl));
        Mockito.when(result.getGroupAccessControl()).thenReturn(new HashSet<>(groupAccessControl));
        return result;
    }

    public static Port getPublicPort(
            final String id,
            final String name,
            final String comments,
            final String versionedId,
            final ScheduledState scheduledState,
            final boolean isValid,
            final boolean isAuthorized,
            final List<String> userAccessControl,
            final List<String> groupAccessControl) {
        final PublicPort result = getPort(PublicPort.class, id, name, Optional.of(versionedId), comments, scheduledState, isValid, isAuthorized);
        Mockito.when(result.getUserAccessControl()).thenReturn(new HashSet<>(userAccessControl));
        Mockito.when(result.getGroupAccessControl()).thenReturn(new HashSet<>(groupAccessControl));
        return result;
    }

    // Funnel

    public static Funnel getFunnel(
            final String id,
            final Optional<String> versionedId,
            final boolean isAuthorized) {
     return getFunnel(id, versionedId.orElse(null), null, isAuthorized);
    }

    public static Funnel getFunnel(
        final String id,
        final String versionedId,
        String comments,
        final boolean isAuthorized
    ) {
        final Funnel result = Mockito.mock(Funnel.class);

        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getVersionedComponentId()).thenReturn(Optional.ofNullable(versionedId));
        Mockito.when(result.getComments()).thenReturn(comments);

        setAuthorized(result, isAuthorized);
        return result;
    }

    // Label

    public static Label getLabel(
            final String id,
            final String value,
            final boolean isAuthorized) {
        final Label result = Mockito.mock(Label.class);

        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getValue()).thenReturn(value);

        setAuthorized(result, isAuthorized);
        return result;
    }

    // Parameter context

    public static ParameterContext getParameterContext(
            final String id,
            final String name,
            final String description,
            final Map<ParameterDescriptor, Parameter> parameters,
            final boolean isAuthorized) {
        final ParameterContext result = Mockito.mock(ParameterContext.class);

        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getDescription()).thenReturn(description);
        Mockito.when(result.getParameters()).thenReturn(parameters);

        setAuthorized(result, isAuthorized);
        return result;
    }

    public static Parameter getParameter(final String name, final String value, final boolean isSensitive, final String description) {
        final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(name).description(description).sensitive(isSensitive).build();
        return new Parameter(descriptor, value);

    }

    // ControllerServiceNode

    public static ControllerServiceNode getControllerServiceNode(
            final String id,
            final String name,
            final String comments,
            final Map<PropertyDescriptor, String> rawProperties,
            final boolean isAuthorized) {
        return getControllerServiceNode(id, name, comments, rawProperties, null, isAuthorized);
    }

    public static ControllerServiceNode getControllerServiceNode(
            final String id,
            final String name,
            final String comments,
            final Map<PropertyDescriptor, String> rawProperties,
            final String versionedComponentId,
            final boolean isAuthorized) {
        final ControllerService controllerService = Mockito.mock(ControllerService.class);
        final ControllerServiceNode result = Mockito.mock(ControllerServiceNode.class);
        Mockito.doReturn(controllerService).when(result).getControllerServiceImplementation();

        // set controller service node attributes
        Mockito.when(result.getIdentifier()).thenReturn(id);
        Mockito.when(result.getName()).thenReturn(name);
        Mockito.when(result.getVersionedComponentId()).thenReturn(Optional.ofNullable(versionedComponentId));
        Mockito.when(result.getComments()).thenReturn(comments);
        Mockito.when(result.getRawPropertyValues()).thenReturn(rawProperties);

        setAuthorized(result, isAuthorized);
        return result;
    }

    // Helper

    private static void setAuthorized(final Authorizable authorizable, final boolean isAuthorized) {
        Mockito.when(authorizable.isAuthorized(
                Mockito.any(Authorizer.class),
                Mockito.any(RequestAction.class),
                AdditionalMatchers.or(Mockito.any(NiFiUser.class), Mockito.isNull()))
        ).thenReturn(isAuthorized);
    }

    public static Collection<Relationship> getBasicRelationships() {
        final Collection<Relationship> result = new HashSet<>();
        result.add(new Relationship.Builder().name("success").description("Success").build());
        result.add(new Relationship.Builder().name("failure").description("Failure").build());
        return result;
    }

     public interface DummyProcessor extends Processor {}
     public interface DummyFlowFilePrioritizer extends FlowFilePrioritizer {}
}
