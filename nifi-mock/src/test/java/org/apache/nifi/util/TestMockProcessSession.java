package org.apache.nifi.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class TestMockProcessSession {

    @Test(expected = AssertionError.class)
    public void testPenalizeFlowFileFromProcessor() {
        TestRunners.newTestRunner(PoorlyBehavedProcessor.class).run();
    }

    protected static class PoorlyBehavedProcessor extends AbstractProcessor {

        private static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .build();

        private final Set<Relationship> relationships = Collections.singleton(REL_FAILURE);

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(final ProcessContext ctx, final ProcessSession session) throws ProcessException {
            final FlowFile file = session.create();
            session.penalize(file);
            session.transfer(file, REL_FAILURE);
        }

    }
}
