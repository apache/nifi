package org.apache.nifi.processors.azure;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractAzureTableProcessor extends AbstractAzureProcessor {

    public static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder().name("Table").description("Table name in Azure").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true).required(true).build();
    public static final PropertyDescriptor ROW = new PropertyDescriptor.Builder().name("Row key").description("Table row key in Azure").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(true).required(true).build();
    public static final PropertyDescriptor PARTITION = new PropertyDescriptor.Builder().name("Partition key").description("Table partition key in Azure")
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).build();
    public static final List<PropertyDescriptor> properties = Collections
                .unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, TABLE, ROW, PARTITION));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    
}
