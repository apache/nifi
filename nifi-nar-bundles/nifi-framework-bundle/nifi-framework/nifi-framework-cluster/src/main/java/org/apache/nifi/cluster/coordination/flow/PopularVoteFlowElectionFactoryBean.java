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

package org.apache.nifi.cluster.coordination.flow;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

public class PopularVoteFlowElectionFactoryBean implements FactoryBean<PopularVoteFlowElection> {
    private static final Logger logger = LoggerFactory.getLogger(PopularVoteFlowElectionFactoryBean.class);
    private NiFiProperties properties;

    @Override
    public PopularVoteFlowElection getObject() throws Exception {
        final String maxWaitTime = properties.getFlowElectionMaxWaitTime();
        long maxWaitMillis;
        try {
            maxWaitMillis = FormatUtils.getTimeDuration(maxWaitTime, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            logger.warn("Failed to parse value of property '{}' as a valid time period. Value was '{}'. Ignoring this value and using the default value of '{}'",
                NiFiProperties.FLOW_ELECTION_MAX_WAIT_TIME, maxWaitTime, NiFiProperties.DEFAULT_FLOW_ELECTION_MAX_WAIT_TIME);
            maxWaitMillis = FormatUtils.getTimeDuration(NiFiProperties.DEFAULT_FLOW_ELECTION_MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
        }

        final Integer maxNodes = properties.getFlowElectionMaxCandidates();

        final StringEncryptor encryptor = StringEncryptor.createEncryptor(properties);
        final FingerprintFactory fingerprintFactory = new FingerprintFactory(encryptor);
        return new PopularVoteFlowElection(maxWaitMillis, TimeUnit.MILLISECONDS, maxNodes, fingerprintFactory);
    }

    @Override
    public Class<?> getObjectType() {
        return PopularVoteFlowElection.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }
}
