
// IntelliJ API Decompiler stub source generated from a class file
// Implementation of methods is not available

package org.apache.nifi.aws.schemaregistry.serde;

public class GlueSchemaRegistryIncompatibleDataException extends RuntimeException {
    public static final java.lang.String UNKNOWN_DATA_ERROR_MESSAGE = "Data is not compatible with schema registry";
    public static final java.lang.String UNKNOWN_HEADER_VERSION_BYTE_ERROR_MESSAGE = "Invalid schema registry header version byte in data";
    public static final java.lang.String UNKNOWN_COMPRESSION_BYTE_ERROR_MESSAGE = "Invalid schema registry compression byte in data";

    public GlueSchemaRegistryIncompatibleDataException(java.lang.String message) {
        super(message);
    }
}