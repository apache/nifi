package org.apache.nifi.processors.adx.enums;

public enum RelationshipStatusEnum {
    RL_SUCCEEDED("RL_SUCCEEDED","Relationship for success"),
    RL_FAILED("RL_FAILED","Relationship for failure");

    private String displayName;
    private String description;


    RelationshipStatusEnum(String displayName, String description) {
        this.displayName = displayName;
        this.description = description;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }
}
