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
package org.apache.nifi.leaderelection.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({"Election", "Leader", "Zookeeper"})
@CapabilityDescription(" Leader//Follower based election using Zookeeper")
public class ZookeeperElectionService extends AbstractControllerService implements ZookeeperElection {
    public static final PropertyDescriptor zkHosts = new PropertyDescriptor
            .Builder().name("Zookeeper Hosts")
            .description("Hosts String for connecting to Zookeeper ie: host:port,host2:port,host3:port")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor sessionTimeout = new PropertyDescriptor
            .Builder().name("Timeout")
            .description("Amount of time to wait for heartbeats from Zookeeper before delaring the session invalid")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final Pattern zkElectionPathRegEx = Pattern.compile("^\\/.*\\w[\\/]{0}$");
    public static final PropertyDescriptor zkElectionNode = new PropertyDescriptor
            .Builder().name("Zookeeper Election Path")
            .description("The absolute znode path to use for the election. Voters will show up under this node. ie: /elections/election123")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(zkElectionPathRegEx))
            .expressionLanguageSupported(false)
            .build();
    private static final List<PropertyDescriptor> properties;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(zkHosts);
        props.add(sessionTimeout);
        props.add(zkElectionNode);
        properties = Collections.unmodifiableList(props);
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    private volatile ZookeeperElectionProcess election;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException {
               election = new ZookeeperElectionProcess(context.getProperty(zkElectionNode).getValue(), context.getProperty(zkHosts).getValue() ,context.getProperty(sessionTimeout).asInteger());
               election.run();
    }
    @OnDisabled
    public void shutdown() {
         election.destroy();
         election = null;
    }
     @Override
     public boolean isLeader() {
          return election.isLeader();
     }
     @Override
     public List<String> aliveElectors() {
          return election.aliveElectors();
     }
     @Override
     public long LastElection() {
          return election.LastElection();
     }
     @Override
     public String ID() {
          return election.ID();
     }
}