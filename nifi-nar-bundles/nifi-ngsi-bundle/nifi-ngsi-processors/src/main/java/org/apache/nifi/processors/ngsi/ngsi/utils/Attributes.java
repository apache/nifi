package org.apache.nifi.processors.ngsi.ngsi.utils;

import java.util.ArrayList;

public class Attributes {
    public String attrName;
    public String attrType;
    public String attrValue;
    public ArrayList<Metadata> attrMetadata;
    public String metadataString;

    public String getMetadataString() {
        return metadataString;
    }

    public void setMetadataString(String metadataString) {
        this.metadataString = metadataString;
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

    public ArrayList<Metadata> getAttrMetadata() {
        return attrMetadata;
    }

    public void setAttrMetadata(ArrayList<Metadata> attrMetadata) {
        this.attrMetadata = attrMetadata;
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

    public Attributes(String attrName, String attrType, String attrValue, ArrayList<Metadata> attrMetadata,String metadataString) {
        this.attrName = attrName;
        this.attrType = attrType;
        this.attrValue = attrValue;
        this.attrMetadata = attrMetadata;
        this.metadataString = metadataString;
    }
}
