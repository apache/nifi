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
package org.apache.nifi.processors.azure;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractAzureBlobProcessor extends AbstractAzureProcessor {

    public static final PropertyDescriptor BLOB = new PropertyDescriptor.Builder().name("blob").displayName("Blob").description("The filename of the blob")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).required(true).defaultValue("${azure.blobname}").build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections
            .unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, BLOB));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

}
