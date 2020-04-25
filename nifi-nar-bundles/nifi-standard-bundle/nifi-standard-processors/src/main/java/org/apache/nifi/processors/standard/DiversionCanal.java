package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * The DiversionCanal processor receives a FlowFile and routes it to the primary relationship if it's available. If
 * that relationship is not available, the FlowFile is routed to the secondary relationship. If none of the two
 * relationships are available, the process will yield.
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@TriggerWhenAnyDestinationAvailable
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"routing"})
@CapabilityDescription("Routes FlowFiles to the primary relationship when it's available. If the primary" +
        "relationship is not available it will route the FlowFiles to the secondary relationship. As soon" +
        "as the primary relationship is available it will again route FlowFiles to the primary relationship")
public class DiversionCanal extends AbstractProcessor {

    public static final Relationship REL_PRIMARY = new Relationship.Builder()
            .name("primary")
            .description("FlowFiles are routed to the primary relationship when it's available")
            .build();
    public static final Relationship REL_SECONDARY = new Relationship.Builder()
            .name("secondary")
            .description("FlowFiles will be routed to secondary relationship while the primary relationship is not available")
            .build();

    private final Set<Relationship> relationships = new HashSet<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        relationships.add(REL_PRIMARY);
        relationships.add(REL_SECONDARY);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        Optional<Relationship> relationship = getRelationship(context.getAvailableRelationships());

        if (!relationship.isPresent()) {
            context.yield();
            session.rollback();
            return;
        }

        relationship.ifPresent(r -> {
            session.getProvenanceReporter().route(flowFile, r);
            session.transfer(flowFile, r);
        });
    }

    private static Optional<Relationship> getRelationship(Set<Relationship> availableRelationships) {
        if (availableRelationships.contains(REL_PRIMARY)) {
            return Optional.of(REL_PRIMARY);
        } else if (availableRelationships.contains(REL_SECONDARY)) {
            return Optional.of(REL_SECONDARY);
        } else {
            return Optional.empty();
        }
    }
}
