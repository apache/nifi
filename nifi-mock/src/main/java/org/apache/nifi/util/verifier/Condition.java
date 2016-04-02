package org.apache.nifi.util.verifier;

import org.apache.nifi.util.MockFlowFile;

public interface Condition {

    public boolean check(MockFlowFile mockFlowFile);
}
