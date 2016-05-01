package org.apache.nifi.processors.azure;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureConstants {
    public static final AllowableValue BLOCK = new AllowableValue("Block", "Block", "Block type blobs");
    public static final AllowableValue PAGE = new AllowableValue("Page", "Page", "Page type blobs");
    
    public static final PropertyDescriptor ACCOUNT_KEY = new PropertyDescriptor.Builder().name("Storage Account Key").description("The storage account key")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).sensitive(true).build();

    public static final PropertyDescriptor ACCOUNT_NAME = new PropertyDescriptor.Builder().name("Storage Account Name").description("The storage account name")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).sensitive(true).build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder().name("Container name").description("Name of the azure storage container")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).build();

}
