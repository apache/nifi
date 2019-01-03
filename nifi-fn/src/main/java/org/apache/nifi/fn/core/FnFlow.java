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
package org.apache.nifi.fn.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.nar.*;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.*;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NiFiProperties;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class FnFlow {

    public static final String REGISTRY = "nifi_registry";
    public static final String BUCKETID = "nifi_bucket";
    public static final String FLOWID = "nifi_flow";
    public static final String FLOWVERSION = "nifi_flowversion";
    public static final String MATERIALIZECONTENT = "nifi_materializecontent";
    public static final String FAILUREPORTS = "nifi_failureports";
    public static final String FLOWFILES = "nifi_flowfiles";
    public static final String CONTENT = "nifi_content";
    public static final List<String> reservedFields = Arrays.asList(REGISTRY,BUCKETID,FLOWID,FLOWVERSION,FAILUREPORTS,MATERIALIZECONTENT,FLOWFILES);

    private List<FnProcessorWrapper> roots;
    private volatile boolean stopRequested = false;
    private FnProcessorWrapper inputProcessor = null;

    public FnFlow(String registryUrl, String bucketID, String flowID, int versionID, VariableRegistry variableRegistry, List<String> failureOutputPorts, boolean materializeContent) throws IOException, NiFiRegistryException, IllegalAccessException, ProcessorInstantiationException, InvocationTargetException, InitializationException {
        this(
                new RegistryUtil(registryUrl).getFlowByID(bucketID, flowID, versionID),
                variableRegistry,
                failureOutputPorts,
                materializeContent
        );
    }
    public FnFlow(String registryUrl, String bucketID, String flowID, VariableRegistry variableRegistry, List<String> failureOutputPorts, boolean materializeContent) throws IOException, NiFiRegistryException, IllegalAccessException, ProcessorInstantiationException, InvocationTargetException, InitializationException {
        this(
                new RegistryUtil(registryUrl).getFlowByID(bucketID, flowID),
                variableRegistry,
                failureOutputPorts,
                materializeContent
        );
    }
    public FnFlow(VersionedFlowSnapshot flowSnapshot, VariableRegistry variableRegistry, List<String> failureOutputPorts, boolean materializeContent) throws IllegalAccessException, ProcessorInstantiationException, InvocationTargetException, InitializationException {

        VersionedProcessGroup contents = flowSnapshot.getFlowContents();
        Set<VersionedProcessor> processors = contents.getProcessors();
        Set<VersionedConnection> connections = contents.getConnections();
        Set<VersionedPort> inputPorts = contents.getInputPorts();
        Set<VersionedFunnel> funnels = contents.getFunnels();

        if(inputPorts.size() > 1)
            throw new IllegalArgumentException("Only one input port per flow is allowed");

        FnControllerServiceLookup serviceLookup = new FnControllerServiceLookup();

        Set<VersionedControllerService> controllerServices = contents.getControllerServices();
        for(VersionedControllerService service : controllerServices){
            serviceLookup.addControllerService(service);
        }

        Map<String, FnProcessorWrapper> pwMap = new HashMap<>();
        for(VersionedConnection connection : connections) {
            boolean isInputPortConnection = false;
            FnProcessorWrapper sourcePw = null;

            ConnectableComponent source = connection.getSource();
            switch (source.getType()){
                case PROCESSOR:
                    if(pwMap.containsKey(source.getId())) {
                        sourcePw = pwMap.get(source.getId());
                    } else {
                        Optional<VersionedProcessor> processor = processors.stream().filter(p -> source.getId().equals(p.getIdentifier())).findFirst();
                        if (processor.isPresent()) {
                            sourcePw = new FnProcessorWrapper(processor.get(), null, serviceLookup, variableRegistry, materializeContent);
                            pwMap.put(source.getId(), sourcePw);
                        } else {
                            throw new IllegalArgumentException("Unknown input processor.. "+source.getId());
                        }
                    }
                    break;
                case REMOTE_INPUT_PORT:
                    isInputPortConnection = true;
                    break;
                case REMOTE_OUTPUT_PORT:
                    throw new IllegalArgumentException("Unsupported source type: "+source.getType());
                case INPUT_PORT:
                    isInputPortConnection = true;
                    break;
                case OUTPUT_PORT:
                    throw new IllegalArgumentException("Unsupported source type: "+source.getType());
                case FUNNEL:
                    throw new IllegalArgumentException("Unsupported source type: "+source.getType());
            }

            ConnectableComponent destination = connection.getDestination();

            FnProcessorWrapper destinationPw;
            switch (destination.getType()) {

                case PROCESSOR:
                    if (pwMap.containsKey(destination.getId())) {
                        destinationPw = pwMap.get(destination.getId());
                    } else {
                        Optional<VersionedProcessor> processor = processors.stream().filter(p -> destination.getId().equals(p.getIdentifier())).findFirst();
                        if (!processor.isPresent())
                            return;

                        destinationPw = new FnProcessorWrapper(processor.get(), sourcePw, serviceLookup, variableRegistry, materializeContent);
                        pwMap.put(destination.getId(), destinationPw);
                    }
                    destinationPw.incomingConnections.add(connection.getIdentifier());

                    if(isInputPortConnection){
                        inputProcessor = destinationPw;
                    } else {
                        destinationPw.parents.add(sourcePw);
                        //Link source and destination
                        for (String relationship : connection.getSelectedRelationships()) {
                            sourcePw.addChild(destinationPw, new Relationship.Builder().name(relationship).build());
                        }
                    }
                    break;
                case INPUT_PORT:
                    throw new IllegalArgumentException("Unsupported destination type: "+destination.getType());
                case REMOTE_INPUT_PORT:
                    throw new IllegalArgumentException("Unsupported destination type: "+destination.getType());
                case REMOTE_OUTPUT_PORT:
                case OUTPUT_PORT:
                    if(isInputPortConnection)
                        throw new IllegalArgumentException("Input ports can not be mapped directly to output ports...");

                    //Link source and destination
                    for (String relationship : connection.getSelectedRelationships()) {
                        sourcePw.addOutputPort(
                                new Relationship.Builder().name(relationship).build(),
                                failureOutputPorts.contains(destination.getId())
                        );
                    }
                    break;
                case FUNNEL:
                    throw new IllegalArgumentException("Unsupported destination type: "+destination.getType());
            }
        }
        roots = pwMap.entrySet()
                .stream()
                .filter(e->e.getValue().parents.isEmpty())
                .map(e->e.getValue())
                .collect(Collectors.toList());
    }
    public FnFlow(FnProcessorWrapper root){
        this(Collections.singletonList(root));
    }
    public FnFlow(List<FnProcessorWrapper> roots){
        this.roots = roots;
    }

    public boolean run(Queue<FnFlowFile> output){
        while(!this.stopRequested){
            for (FnProcessorWrapper pw : roots){
                boolean successful = pw.runRecursive(output);
                if(!successful)
                    return false;
            }
        }
        return true;
    }
    public boolean runOnce(Queue<FnFlowFile> output){
        for (FnProcessorWrapper pw : roots){
            boolean successful = pw.runRecursive(output);
            if(!successful)
                return false;
        }
        return true;
    }
    public void shutdown(){
        this.stopRequested = true;
        this.roots.forEach(r->r.shutdown());
    }

    public static FnFlow createAndEnqueueFromJSON(JsonObject args) throws IllegalAccessException, InvocationTargetException, InitializationException, IOException, ProcessorInstantiationException, NiFiRegistryException {
        if(args == null)
            throw new IllegalArgumentException("Flow arguments can not be null");

        System.out.println("Running flow from json: "+args.toString());

        if(!args.has(REGISTRY) || !args.has(BUCKETID) || !args.has(FLOWID))
            throw new IllegalArgumentException("The following parameters must be provided: "+REGISTRY+", "+BUCKETID+", "+FLOWID);

        String registryurl = args.getAsJsonPrimitive(REGISTRY).getAsString();
        String bucketID = args.getAsJsonPrimitive(BUCKETID).getAsString();
        String flowID = args.getAsJsonPrimitive(FLOWID).getAsString();

        int flowVersion = -1;
        if(args.has(FLOWVERSION))
            flowVersion = args.getAsJsonPrimitive(FLOWVERSION).getAsInt();

        boolean materializeContent = true;
        if(args.has(MATERIALIZECONTENT))
            materializeContent = args.getAsJsonPrimitive(MATERIALIZECONTENT).getAsBoolean();

        List<String> failurePorts = new ArrayList<>();
        if(args.has(FAILUREPORTS))
            args.getAsJsonArray(FAILUREPORTS).forEach(port->
                    failurePorts.add(port.getAsString())
            );

        Map<VariableDescriptor,String> inputVariables = new HashMap<>();

        args.entrySet().forEach(entry ->{
            if(!reservedFields.contains(entry.getKey()))
                inputVariables.put(new VariableDescriptor(entry.getKey()), entry.getValue().getAsString());
        });

        FnFlow flow = new FnFlow(registryurl,bucketID,flowID,flowVersion,()->inputVariables, failurePorts, materializeContent);
        flow.enqueueFromJSON(args);
        return flow;
    }

    public void enqueueFlowFile(byte[] content, Map<String,String> attributes){

        if(inputProcessor == null)
            throw new IllegalArgumentException("Flow does not have an input port...");

        //enqueue data
        Queue<FnFlowFile> input = new LinkedList<>();
        input.add(new FnFlowFile(content,attributes,inputProcessor.materializeContent));

        inputProcessor.enqueueAll(input);
    }
    public void enqueueFromJSON(JsonObject json){

        if(inputProcessor == null)
            throw new IllegalArgumentException("Flow does not have an input port...");

        Queue<FnFlowFile> input = new LinkedList<>();
        JsonArray flowFiles = json.getAsJsonArray(FLOWFILES);
        flowFiles.forEach(f->{
            JsonObject file = f.getAsJsonObject();

            String content = file.getAsJsonPrimitive(CONTENT).getAsString();

            Map<String,String> attributes = new HashMap<>();
            file.entrySet().forEach(entry ->{
                if(!CONTENT.equals(entry.getKey()))
                    attributes.put(entry.getKey(), entry.getValue().getAsString());
            });
            input.add(new FnFlowFile(content,attributes,inputProcessor.materializeContent));
        });

        //enqueue data
        inputProcessor.enqueueAll(input);
    }
}
