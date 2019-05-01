package org.apache.nifi.grok;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.SchemaRegistryService;

import java.util.ArrayList;
import java.util.List;

public class TestRegistryProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(new PropertyDescriptor.Builder()
                .name("Schema Registry Service test processor")
                .description("Schema Registry Service test processor")
                .identifiesControllerService(SchemaRegistryService.class)
                .required(true)
                .build());
        return properties;
    }

}
