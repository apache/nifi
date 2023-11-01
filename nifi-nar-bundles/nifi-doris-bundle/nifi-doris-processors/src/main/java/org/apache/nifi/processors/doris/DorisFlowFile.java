package org.apache.nifi.processors.doris;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFile;

import java.util.Objects;

public class DorisFlowFile {
    private String srcDatabaseName;
    private String srcTableName;
    private String columns;
    private FlowFile flowFile;

    private JsonNode jsonNode;

    /*public DorisFlowFile(String srcDatabaseName, String srcTableName, String columns, FlowFile flowFile, JsonNode jsonData) {
        this.srcDatabaseName = srcDatabaseName;
        this.srcTableName = srcTableName;
        this.columns = columns;
        this.flowFile = flowFile;
        this.jsonData = jsonData;
    }*/

    public String getSrcDatabaseName() {
        return srcDatabaseName;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public String getColumns() {
        return columns;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public JsonNode getJsonNode() {
        return jsonNode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DorisFlowFile that = (DorisFlowFile) o;

        if (!Objects.equals(srcDatabaseName, that.srcDatabaseName))
            return false;
        if (!Objects.equals(srcTableName, that.srcTableName)) return false;
        if (!Objects.equals(columns, that.columns)) return false;
        if (!Objects.equals(flowFile, that.flowFile)) return false;
        return Objects.equals(jsonNode, that.jsonNode);
    }

    @Override
    public int hashCode() {
        int result = srcDatabaseName != null ? srcDatabaseName.hashCode() : 0;
        result = 31 * result + (srcTableName != null ? srcTableName.hashCode() : 0);
        result = 31 * result + (columns != null ? columns.hashCode() : 0);
        result = 31 * result + (flowFile != null ? flowFile.hashCode() : 0);
        result = 31 * result + (jsonNode != null ? jsonNode.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DorisFlowFile{" +
                "srcDatabaseName='" + srcDatabaseName + '\'' +
                ", srcTableName='" + srcTableName + '\'' +
                ", columns='" + columns + '\'' +
                ", flowFile=" + flowFile +
                ", jsonData=" + jsonNode +
                '}';
    }

    public void setSrcDatabaseName(String srcDatabaseName) {
        this.srcDatabaseName = srcDatabaseName;
    }

    public void setSrcTableName(String srcTableName) {
        this.srcTableName = srcTableName;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public void setFlowFile(FlowFile flowFile) {
        this.flowFile = flowFile;
    }

    public void setJsonNode(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }
}
