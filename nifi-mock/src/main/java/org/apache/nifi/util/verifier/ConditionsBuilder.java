package org.apache.nifi.util.verifier;

import java.util.ArrayList;

public class ConditionsBuilder {

    protected final ArrayList<Condition> conditions = new ArrayList<>();

    public ConditionsBuilder(Condition condition) {
        conditions.add(condition);
    }

    static public ConditionsBuilder attributeEqual(String name, String value) {
        return new ConditionsBuilder(new AttributeEqual(name,value));
    }

    static public ConditionsBuilder contentEqual(String content) {
        return new ConditionsBuilder(new ContentEqual(content));
    }

    public ConditionsBuilder andAttributeEqual(String name, String value) {
        conditions.add(new AttributeEqual(name,value));
        return this;
    }

    public ConditionsBuilder andContentEqual(String content) {
        conditions.add(new ContentEqual(content));
        return this;
    }

    public ArrayList<Condition> getConditions() {
        return conditions;
    }


}
