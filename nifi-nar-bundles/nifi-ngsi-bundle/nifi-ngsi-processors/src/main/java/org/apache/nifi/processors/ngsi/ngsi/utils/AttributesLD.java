package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class AttributesLD {
    public String attrName;
    public String attrType;
    public String attrValue;
    public boolean hasSubAttrs;
    public ArrayList<AttributesLD> subAttrs;

    public boolean isHasSubAttrs() {
        return hasSubAttrs;
    }

    public ArrayList<AttributesLD> getSubAttrs() {
        return subAttrs;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getAttrType() {
        return attrType;
    }

    public void setAttrType(String attrType) {
        this.attrType = attrType;
    }

    public String getAttrValue() {
        return attrValue;
    }

    public void setAttrValue(String attrValue) {
        this.attrValue = attrValue;
    }

    public AttributesLD(String attrName, String attrType, String attrValue, boolean hasSubAttrs, ArrayList<AttributesLD> subAttrs) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.attrValue = attrValue;
        this.hasSubAttrs = hasSubAttrs;
        this.subAttrs= subAttrs;

    }
}
