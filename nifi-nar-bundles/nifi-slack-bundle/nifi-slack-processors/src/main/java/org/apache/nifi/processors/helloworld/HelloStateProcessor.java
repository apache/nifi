package org.apache.nifi.processors.helloworld;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Tags("state")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Stateful(scopes = Scope.CLUSTER, description = "")
public class HelloStateProcessor extends AbstractProcessor {

    private static final String COUNTER_KEY = "counter";
    private static final String TIMESTAMP_KEY = "timestamp";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        getLogger().info("init");
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        getLogger().info("customValidate");
        return Collections.emptyList();
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) throws IOException {
        getLogger().info("onScheduled");
        if (getNodeTypeProvider().isPrimary()) {
            final StateManager stateManager = context.getStateManager();
            final StateMap state = stateManager.getState(Scope.CLUSTER);

            if (!state.getStateVersion().isPresent()) {
                stateManager.setState(new HashMap<>(), Scope.CLUSTER);
            }
        }
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("onUnscheduled");
    }

    @OnStopped
    public void onStopped() {
        getLogger().info("onStopped");
    }

    @OnShutdown
    public void onShutdown() {
        getLogger().info("onShutdown");
    }

//    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
//        try {
//            getLogger().info("onTrigger");
//
//            FlowFile flowFile = session.get();
//            if (flowFile == null) {
//                getLogger().info("Null FlowFile");
//                return;
//            }
//
//            StateMap oldState = session.getState(Scope.CLUSTER);
//
//            Map<String, String> stateMap = new HashMap<>(oldState.toMap());
//
//            String counterStr = stateMap.get(COUNTER_KEY);
//
//            int counter = counterStr != null ? Integer.parseInt(counterStr) : 0;
//            counter++;
//            stateMap.put(COUNTER_KEY, Integer.toString(counter));
//
//            stateMap.put(TIMESTAMP_KEY, LocalDateTime.now().toString());
//
//            boolean success = session.replaceState(oldState, stateMap, Scope.CLUSTER); // reread state
//
//            if (success) {
//                session.transfer(flowFile, REL_SUCCESS);
//            } else {
//                session.transfer(flowFile, REL_FAILURE);
//            }
//        } catch (Exception e) {
//            getLogger().error("HelloWorldProcessor failure", e);
//            throw new ProcessException("HelloWorldProcessor failure", e);
//        }
//    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            getLogger().info("onTrigger");

            FlowFile flowFile = session.get();
            if (flowFile == null) {
                getLogger().info("Null FlowFile");
                return;
            }

            StateManager stateManager = context.getStateManager();

            StateMap oldState = stateManager.getState(Scope.CLUSTER);

            Map<String, String> stateMap = new HashMap<>(oldState.toMap());

            String counterStr = stateMap.get(COUNTER_KEY);

            int counter = counterStr != null ? Integer.parseInt(counterStr) : 0;
            counter++;
            stateMap.put(COUNTER_KEY, Integer.toString(counter));

            boolean success = stateManager.replace(oldState, stateMap, Scope.CLUSTER);

            if (success) {
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);

            }
        } catch (Exception e) {
            getLogger().error("HelloWorldProcessor failure", e);
            throw new ProcessException("HelloWorldProcessor failure", e);
        }
    }
}
