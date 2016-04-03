package org.apache.nifi.util.verifier;

import java.util.List;

public class Conditions {

    protected final List<Condition> conditions;

    public Conditions(List<Condition> conditions) {
        super();
        this.conditions = conditions;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public String toString() {
        return "Conditions [conditions=" + conditions + "]";
    }
}
