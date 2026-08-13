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
package org.apache.nifi.processors.smb.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.services.smb.SmbClientProvider;

import java.net.URI;
import java.util.Map;

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_TERMINATE;

public class ServiceLocationFlowFileFilter implements FlowFileFilter {

    private final SmbClientProvider clientProvider;
    private final int batchSize;

    private URI selectedServiceLocation;
    private Map<String, String> selectedAttributes;
    private int count = 0;

    public ServiceLocationFlowFileFilter(SmbClientProvider clientProvider, int batchSize) {
        this.clientProvider = clientProvider;
        this.batchSize = batchSize;
    }

    @Override
    public FlowFileFilterResult filter(FlowFile flowFile) {
        final Map<String, String> attributes = flowFile.getAttributes();
        final URI serviceLocation = clientProvider.getServiceLocation(attributes);

        if (selectedServiceLocation == null) {
            selectedServiceLocation = serviceLocation;
            selectedAttributes = attributes;
        }

        if (count >= batchSize) {
            return REJECT_AND_TERMINATE;
        }

        if (selectedServiceLocation.equals(serviceLocation)) {
            count += 1;
            return ACCEPT_AND_CONTINUE;
        } else {
            return REJECT_AND_CONTINUE;
        }
    }

    public URI getSelectedServiceLocation() {
        return selectedServiceLocation;
    }

    public Map<String, String> getSelectedAttributes() {
        return selectedAttributes;
    }
}
