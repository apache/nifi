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
package org.apache.nifi.processors.aws.ec2;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;

public abstract class AbstractEC2Processor extends AbstractAwsProcessor<Ec2Client, Ec2ClientBuilder> {
    @Override
    protected Ec2ClientBuilder createClientBuilder(ProcessContext context) {
        return Ec2Client.builder();
    }

}
