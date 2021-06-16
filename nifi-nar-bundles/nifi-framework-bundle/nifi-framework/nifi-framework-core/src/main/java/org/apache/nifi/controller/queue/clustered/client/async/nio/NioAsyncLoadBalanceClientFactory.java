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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientFactory;
import org.apache.nifi.events.EventReporter;

import javax.net.ssl.SSLContext;

public class NioAsyncLoadBalanceClientFactory implements AsyncLoadBalanceClientFactory {
    private final SSLContext sslContext;
    private final int timeoutMillis;
    private final FlowFileContentAccess flowFileContentAccess;
    private final EventReporter eventReporter;
    private final LoadBalanceFlowFileCodec flowFileCodec;

    public NioAsyncLoadBalanceClientFactory(final SSLContext sslContext, final int timeoutMillis, final FlowFileContentAccess flowFileContentAccess, final EventReporter eventReporter,
                                            final LoadBalanceFlowFileCodec loadBalanceFlowFileCodec) {
        this.sslContext = sslContext;
        this.timeoutMillis = timeoutMillis;
        this.flowFileContentAccess = flowFileContentAccess;
        this.eventReporter = eventReporter;
        this.flowFileCodec = loadBalanceFlowFileCodec;
    }


    @Override
    public NioAsyncLoadBalanceClient createClient(final NodeIdentifier nodeIdentifier) {
        return new NioAsyncLoadBalanceClient(nodeIdentifier, sslContext, timeoutMillis, flowFileContentAccess, new StandardLoadBalanceFlowFileCodec(), eventReporter);
    }
}
