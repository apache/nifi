package org.apache.nifi.util.verifier;

import org.apache.nifi.util.MockFlowFile;

public class AttributeEqual implements Condition {

    protected final String name;
    protected final String value;
    
    public AttributeEqual(String name, String value) {
        super();
        this.name = name;
        this.value = value;
    }

    @Override
    public boolean check(MockFlowFile mockFlowFile) {

        return true;
    }

}
