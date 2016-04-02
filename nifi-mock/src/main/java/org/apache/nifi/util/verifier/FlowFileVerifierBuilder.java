package org.apache.nifi.util.verifier;

import java.util.ArrayList;

import org.apache.nifi.processor.Relationship;

public class FlowFileVerifierBuilder {
    
    protected final Relationship relationship;
    protected final ArrayList<Conditions> conditions = new ArrayList<>();

    public FlowFileVerifierBuilder(Relationship relationship) {
        super();
        this.relationship = relationship;
    }
    
}
