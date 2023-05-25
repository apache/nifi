package org.apache.nifi.processors.helloworld;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags("hello")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class HelloWorldProcessor extends AbstractProcessor {

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
    public void onScheduled() {
        getLogger().info("onScheduled");
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

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            getLogger().info("onTrigger");

            FlowFile flowFile = session.get();
            if (flowFile == null) {
                getLogger().info("Null FlowFile");
                return;
            }

//            List<FlowFile> flowFiles = session.get(ff -> {
//                //String uuid = ff.getAttribute(CoreAttributes.UUID.key());
//                String uuid = ff.getAttribute("my.uuid");
//                if (uuid != null && uuid.equals("just this")) {
//                    return FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE;
//                } else {
//                    return FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
//                }
//            });
//            if (flowFiles.isEmpty()) {
//                getLogger().info("Null FlowFile");
//                return;
//            }
//            FlowFile flowFile = flowFiles.get(0);

            String name = flowFile.getAttribute("name");
            if (name == null) {
                name = "Anonymous";
            }

            if ("Exception".equals(name)) {
                throw new ProcessException("exception");
                // => session.rollback(true) in AbstractProcessor (if thrown from onTrigger())
            }

            if ("Yield".equals(name)) {
                context.yield();
                session.remove(flowFile);
                return;
            }

            if ("Penalize".equals(name)) {
                session.penalize(flowFile);
                session.rollback();
                return;
                // => no effect, will be rolled back
                // same as Rollback case below
                // session.rollback(true) should be used or ProcessException should be thrown
            }

            if ("Rollback".equals(name)) {
                session.rollback();
                return;
                // => retriggered immediately
            }

            if ("Failure".equals(name)) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            String greeting = String.format("Hello %s!", name);
            getLogger().info(greeting);

            try (OutputStream os = session.write(flowFile)) {
                os.write(greeting.getBytes());
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("HelloWorldProcessor failure", e);
            throw new ProcessException("HelloWorldProcessor failure", e);
        }
    }
}
