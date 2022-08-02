package org.apache.iotdb.processors.model;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class Schema {
    private TimeType timeType;
    private HashMap<String, Field> fieldMap;
    private ArrayList<String> fieldNames;

    public enum TimeType {
        LONG,
        STRING
    }

    public Schema(String timeType, List<Field> fields) {
        this.timeType = "long".equals(timeType) ? TimeType.LONG : TimeType.STRING;
        this.fieldMap = new HashMap<>();
        this.fieldNames = new ArrayList<>();
        fields.forEach(
                field -> {
                    fieldMap.put(field.getTsName(), field);
                    fieldNames.add(field.getTsName());
                });
    }

    public TimeType getTimeType() {
        return timeType;
    }

    public ArrayList<String> getFieldNames() {
        return fieldNames;
    }

    public List<TSDataType> getDataTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getDataType())
                .collect(Collectors.toList());
    }

    public List<TSEncoding> getEncodingTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getEncoding())
                .collect(Collectors.toList());
    }

    public List<CompressionType> getCompressionTypes() {
        return fieldMap.values().stream()
                .map(field -> field.getCompressionType())
                .collect(Collectors.toList());
    }

    public TSDataType getDataType(String fieldName) {
        return fieldMap.get(fieldName).getDataType();
    }

    public TSEncoding getEncodingType(String fieldName) {
        return fieldMap.get(fieldName).getEncoding();
    }

    public CompressionType getCompressionType(String fieldName) {
        return fieldMap.get(fieldName).getCompressionType();
    }

    public List<Field> getFields() {
        return (List<Field>) fieldMap.values();
    }

    public static Set<String> getSupportedTimeType() {
        return new HashSet<String>() {
            {
                add("long");
                add("string");
            }
        };
    }
}
