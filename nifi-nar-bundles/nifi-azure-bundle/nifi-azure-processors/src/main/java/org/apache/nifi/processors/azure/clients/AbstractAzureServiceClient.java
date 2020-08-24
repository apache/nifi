/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
