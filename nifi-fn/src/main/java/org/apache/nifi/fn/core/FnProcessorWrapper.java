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


import org.apache.nifi.annotation.lifecycle.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.VersionedProcessor;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class FnProcessorWrapper {

    private final long runSchedule = 100;

    public List<FnProcessorWrapper> parents;
    public List<String> incomingConnections;
    public final Map<Relationship, ArrayList<FnProcessorWrapper>> children;
    private final Set<Relationship> autoTermination;
    private final Set<Relationship> successOutputPorts;
    private final Set<Relationship> failureOutputPorts;


    public boolean materializeContent;
    private final Processor processor;
    private final FnProcessContext context;
    private final Queue<FnFlowFile> inputQueue;
    private final VariableRegistry variableRegistry;

    private final Collection<ProvenanceEventRecord> provenanceEvents;

    private final Set<FnProcessSession> createdSessions;
    private final ComponentLog logger;

    private final FnControllerServiceLookup lookup;

    private volatile boolean stopRequested = false;
    private volatile boolean isStopped = true;
    private volatile boolean initialized = false;

    FnProcessorWrapper(final VersionedProcessor processor, final FnProcessorWrapper parent, FnControllerServiceLookup lookup, VariableRegistry registry, boolean materializeContent) throws InvocationTargetException, IllegalAccessException, ProcessorInstantiationException {
        this(ReflectionUtils.createProcessor(processor),parent, lookup, registry,materializeContent);
        for(String relationship : processor.getAutoTerminatedRelationships()) {
            this.addAutoTermination(new Relationship.Builder().name(relationship).build());
        }
        processor.getProperties().forEach((key, value) -> this.setProperty(key,value));
    }

    FnProcessorWrapper(final Processor processor, final FnProcessorWrapper parent, FnControllerServiceLookup lookup, VariableRegistry registry, boolean materializeContent) throws InvocationTargetException, IllegalAccessException {

        this.processor = processor;
        this.parents = new ArrayList<>();
        if(parent != null)
            this.parents.add(parent);
        this.lookup = lookup;
        this.materializeContent = materializeContent;

        this.incomingConnections = new ArrayList<>();
        this.children = new HashMap<>();
        this.autoTermination = new HashSet<>();
        this.successOutputPorts = new HashSet<>();
        this.failureOutputPorts = new HashSet<>();

        this.provenanceEvents = new ArrayList<>();
        this.createdSessions = new CopyOnWriteArraySet<>();
        this.inputQueue = new LinkedList<>();
        this.variableRegistry = registry;
        this.context = new FnProcessContext(processor, lookup, processor.getIdentifier(), new FnStateManager(), variableRegistry);
        this.context.setMaxConcurrentTasks(1);

        final FnProcessorInitializationContext initContext = new FnProcessorInitializationContext(processor, context);
        processor.initialize(initContext);
        logger =  initContext.getLogger();

        ReflectionUtils.invokeMethodsWithAnnotation(OnAdded.class, processor);

        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnConfigurationRestored.class, processor);
    }


    public Processor getProcessor(){return this.processor;}

    private void initialize(){

        //Validate context
        Collection<ValidationResult> validationResult = context.validate();
        if(validationResult.stream().anyMatch(a->!a.isValid()) || !this.validate()) {
            throw new IllegalArgumentException(
                    "context is not valid: "+
                            String.join("\n",validationResult.stream().map(r->r.toString()).collect(Collectors.toList())));
        }
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnScheduled.class, processor, context);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("Exception: ",e);
        }
        initialized = true;
    }
    public boolean runRecursive(Queue<FnFlowFile> output) {
        if(!initialized)
            initialize();

        AtomicBoolean processingSuccess = new AtomicBoolean(true);
        Set<Relationship> outputRelationships = new HashSet<>(children.keySet());
        outputRelationships.addAll(successOutputPorts);
        outputRelationships.addAll(failureOutputPorts);
        do {
            this.isStopped = false;
            AtomicBoolean nextStepCalled = new AtomicBoolean(false);
            try {
                logger.info("Running "+this.processor.getClass().getSimpleName()+".onTrigger with "+inputQueue.size()+" records");
                processor.onTrigger(context, () -> {
                    final FnProcessSession session = new FnProcessSession(
                                                                inputQueue,
                                                                provenanceEvents,
                                                                processor,
                                                                outputRelationships,
                                                                materializeContent,
                                                                () -> {
                                                                    if(!nextStepCalled.get()) {
                                                                        nextStepCalled.set(true);
                                                                        boolean successfulRun = runChildren(output);
                                                                        processingSuccess.set(successfulRun);
                                                                    }
                                                                });
                    createdSessions.add(session);
                    return session;
                });
                if(!nextStepCalled.get()) {
                    nextStepCalled.set(true);
                    boolean successfulRun = runChildren(output);
                    processingSuccess.set(successfulRun);
                }
                provenanceEvents.clear();
                Thread.sleep(runSchedule);
            } catch (final Exception t) {
                logger.error("Exception in runRecursive "+this.processor.getIdentifier(),t);
                return false;
            }
        } while(!stopRequested && !inputQueue.isEmpty() && processingSuccess.get());
        this.isStopped = true;
        return processingSuccess.get();
    }

    private boolean runChildren(Queue<FnFlowFile> output) {
        Queue<FnFlowFile> penalizedFlowFiles = this.getPenalizedFlowFiles();
        if(penalizedFlowFiles.size() > 0){
            output.addAll(penalizedFlowFiles);
            return false;
        }

        for(Relationship r : this.getProcessor().getRelationships()) {
            if(this.autoTermination.contains(r))
                continue;

            Queue<FnFlowFile> files = this.getAndRemoveFlowFilesForRelationship(r);
            if(files.size() == 0)
                continue;

            if(this.failureOutputPorts.contains(r)) {
                output.addAll(files);
                return false;
            }
            if(this.successOutputPorts.contains(r))
                output.addAll(files);

            if(children.containsKey(r)) {
                for (FnProcessorWrapper child : children.get(r)) {
                    child.enqueueAll(files);
                    boolean successfulRun = child.runRecursive(output);
                    if (!successfulRun)
                        return false;
                }
            }
        }
        return true;
    }
    public void shutdown(){
        this.stopRequested = true;
        for(Relationship r : this.getProcessor().getRelationships()) {
            if(this.autoTermination.contains(r))
                continue;

            if(!children.containsKey(r))
                throw new IllegalArgumentException("No child for relationship: "+r.getName());

            children.get(r).forEach(FnProcessorWrapper::shutdown);
        }

        while(!this.isStopped){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        try {
            ReflectionUtils.invokeMethodsWithAnnotation(OnUnscheduled.class, processor, context);

            ReflectionUtils.invokeMethodsWithAnnotation(OnStopped.class, processor, context);

            ReflectionUtils.invokeMethodsWithAnnotation(OnShutdown.class, processor);
        } catch (IllegalAccessException | InvocationTargetException e) {
            logger.error("Failure on shutdown: ", e);
        }
        logger.info(this.processor.getClass().getSimpleName()+" shutdown");
    }
    public boolean validate(){
        if(!context.isValid())
            return false;

        for(Relationship r : this.getProcessor().getRelationships()) {
            boolean hasChildren = this.children.containsKey(r);
            boolean hasAutoterminate = this.autoTermination.contains(r);
            boolean hasFailureOutputPort = this.failureOutputPorts.contains(r);
            boolean hasSuccessOutputPort = this.successOutputPorts.contains(r);
            if (!(hasChildren || hasAutoterminate || hasFailureOutputPort || hasSuccessOutputPort)) {
                logger.error("Processor: " + this.toString() + ", Relationship: " + r.getName() + ", needs either auto terminate, child processors, or an output port");
                return false;
            }
        }
        for( Map.Entry<Relationship, ArrayList<FnProcessorWrapper>> child : this.children.entrySet()){
            for(FnProcessorWrapper n : child.getValue()){
                if(!n.validate())
                    return false;
            }
        }
        return true;
    }


    public void enqueueAll(Queue<FnFlowFile> list){
        inputQueue.addAll(list);
    }
    public Queue<FnFlowFile> getAndRemoveFlowFilesForRelationship(final Relationship relationship) {

        List<FnFlowFile> sortedList = createdSessions.stream()
                .flatMap(s-> s.getAndRemoveFlowFilesForRelationship(relationship).stream())
                .sorted(Comparator.comparing(f -> f.getCreationTime()))
                .collect(Collectors.toList());

        return new LinkedList<>(sortedList);
    }
    public Queue<FnFlowFile> getPenalizedFlowFiles(){
        List<FnFlowFile> sortedList = createdSessions.stream()
                .flatMap(s-> s.getPenalizedFlowFiles().stream())
                .sorted(Comparator.comparing(f -> f.getCreationTime()))
                .collect(Collectors.toList());
        return new LinkedList<>(sortedList);

    }

    public ValidationResult setProperty(final PropertyDescriptor property, final String propertyValue) {
        return context.setProperty(property,propertyValue);
    }
    public ValidationResult setProperty(final String propertyName, final String propertyValue) {
        return context.setProperty(propertyName, propertyValue);
    }
    public void addOutputPort(Relationship relationship, boolean isFailurePort){
        if(isFailurePort)
            this.failureOutputPorts.add(relationship);
        else
            this.successOutputPorts.add(relationship);
    }

    public FnProcessorWrapper addChild(Processor p, Relationship relationship) throws InvocationTargetException, IllegalAccessException {
        ArrayList<FnProcessorWrapper> list = children.computeIfAbsent(relationship, r -> new ArrayList<>());
        FnProcessorWrapper child = new FnProcessorWrapper(p,this, lookup, variableRegistry, materializeContent);
        list.add(child);

        context.addConnection(relationship);
        return child;
    }

    public FnProcessorWrapper addChild(FnProcessorWrapper child, Relationship relationship) {
        ArrayList<FnProcessorWrapper> list = children.computeIfAbsent(relationship, r -> new ArrayList<>());
        list.add(child);

        context.addConnection(relationship);
        return child;
    }
    public void addAutoTermination(Relationship relationship){
        this.autoTermination.add(relationship);

        context.addConnection(relationship);
    }

}

