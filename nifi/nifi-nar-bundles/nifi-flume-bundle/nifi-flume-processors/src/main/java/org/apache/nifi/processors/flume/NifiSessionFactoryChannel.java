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
package org.apache.nifi.processors.flume;

import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;

public class NifiSessionFactoryChannel extends BasicChannelSemantics {

    private final ProcessSessionFactory sessionFactory;
    private final Relationship relationship;

    public NifiSessionFactoryChannel(ProcessSessionFactory sessionFactory, Relationship relationship) {
        this.sessionFactory = sessionFactory;
        this.relationship = relationship;
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        LifecycleState lifecycleState = getLifecycleState();
        if (lifecycleState == LifecycleState.STOP) {
            throw new ChannelFullException("Can't write to a stopped channel");
            //return null;
        }
        return new NifiTransaction(sessionFactory.createSession(), relationship);
    }

    @Override
    public void configure(Context context) {
    }

}
