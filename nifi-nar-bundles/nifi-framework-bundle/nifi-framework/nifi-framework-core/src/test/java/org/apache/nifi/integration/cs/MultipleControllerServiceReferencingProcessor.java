package org.apache.nifi.integration.cs;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.integration.FrameworkIntegrationTest;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MultipleControllerServiceReferencingProcessor extends AbstractProcessor {

    private final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("Counter Service")
            .identifiesControllerService(Counter.class)
            .required(false)
            .build();

    private final PropertyDescriptor ANOTHER_SERVICE = new PropertyDescriptor.Builder()
            .name("Another Counter Service")
            .identifiesControllerService(Counter.class)
            .required(false)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(FrameworkIntegrationTest.REL_SUCCESS);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(new ArrayList<PropertyDescriptor>(){
            {
                add(SERVICE);
                add(ANOTHER_SERVICE);
            }
        });
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException { }
}
