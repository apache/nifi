package org.apache.nifi.processors.ngsi.ngsi.utils;

public class Metadata {
    public String mtdName;
    public String mtdType;
    public String mtdValue;

    public String getMtdName() {
        return mtdName;
    }

    public void setMtdName(String mtdName) {
        this.mtdName = mtdName;
    }

    public String getMtdType() {
        return mtdType;
    }

    public void setMtdType(String mtdType) {
        this.mtdType = mtdType;
    }

    public String getMtdValue() {
        return mtdValue;
    }

    public void setMtdValue(String mtdValue) {
        this.mtdValue = mtdValue;
    }

    public Metadata(String mtdName, String mtdType, String mtdValue) {
        this.mtdName = mtdName;
        this.mtdType = mtdType;
        this.mtdValue = mtdValue;
    }
}
