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
import org.apache.nifi.processor.ProcessContext;

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_TERMINATE;
import static org.apache.nifi.processors.smb.PutSmbFile.HOSTNAME;
import static org.apache.nifi.processors.smb.PutSmbFile.SHARE;

public class HostnameAndShareFlowFileFilter implements FlowFileFilter {

    private final ProcessContext context;
    private final int batchSize;

    private HostSharePair selectedHostSharePair;
    private int count = 0;

    public HostnameAndShareFlowFileFilter(ProcessContext context, int batchSize) {
        this.context = context;
        this.batchSize = batchSize;
    }

    @Override
    public FlowFileFilterResult filter(FlowFile flowFile) {
        final HostSharePair hostSharePair = getFlowFileHostSharePair(flowFile);

        if (selectedHostSharePair == null) {
            selectedHostSharePair = hostSharePair;
        }

        if (count >= batchSize) {
            return REJECT_AND_TERMINATE;
        }

        if (selectedHostSharePair.hostName().equals(hostSharePair.hostName()) && selectedHostSharePair.share().equals(hostSharePair.share())) {
            count += 1;
            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        } else {
            return FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }

    private HostSharePair getFlowFileHostSharePair(final FlowFile flowFile) {
        final String hostName = context.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String share = context.getProperty(SHARE).evaluateAttributeExpressions(flowFile).getValue();

        return new HostSharePair(hostName, share);
    }

    public String getHostName() {
        return selectedHostSharePair.hostName();
    }

    public String getShare() {
        return selectedHostSharePair.share();
    }

    record HostSharePair(String hostName, String share) {
    }
}
