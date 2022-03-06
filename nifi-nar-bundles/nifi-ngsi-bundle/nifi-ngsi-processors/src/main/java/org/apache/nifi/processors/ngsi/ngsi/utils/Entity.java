package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class Entity {
    public String entityId;
    public String entityType;
    public ArrayList<Attributes> entityAttrs;
    public boolean ldVersion;
    public ArrayList<AttributesLD> entityAttrsLD;


    public  Entity(String entityId, String entityType, ArrayList<Attributes> entityAttrs) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.entityAttrs = entityAttrs;
        this.ldVersion=false;
    }
    public  Entity(String entityId, String entityType, ArrayList<AttributesLD> entityAttrsLD,boolean ldVersion) {
        this.entityId = entityId;
        this.entityType = entityType;
        this.ldVersion = true;
        this.entityAttrsLD=entityAttrsLD;
    }

    public boolean isLdVersion() {
        return ldVersion;
    }

    public ArrayList<AttributesLD> getEntityAttrsLD() {
        return entityAttrsLD;
    }

    public void setLdVersion(boolean ldVersion) {
        this.ldVersion = ldVersion;
    }

    public void setEntityAttrsLD(ArrayList<AttributesLD> entityAttrsLD) {
        this.entityAttrsLD = entityAttrsLD;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public ArrayList<Attributes> getEntityAttrs() {
        return entityAttrs;
    }

    public void setEntityAttrs(ArrayList<Attributes> entityAttrs) {
        this.entityAttrs = entityAttrs;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
}
