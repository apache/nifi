package org.apache.nifi.processors.azure.clients;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;

abstract public class AbstractAzureServiceClient<T> {

    protected volatile T client;

    public AbstractAzureServiceClient(PropertyContext context, FlowFile flowFile) {
        setServiceClient(context, flowFile);
    }

    /**
     * Set Azure Service client on object.
     *
     * @param context  Context
     * @param flowFile FlowFile which can hold attributes to use in creation of Azure Service Client.
     */
    abstract protected void setServiceClient(PropertyContext context, FlowFile flowFile);

    /**
     * @return Azure Service Client object.
     */
    protected T getServiceClient() {
        return client;
    }

}
