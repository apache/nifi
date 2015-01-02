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
package org.apache.nifi.processors.kafka;

import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Intended only for local tests to verify functionality.")
public class TestGetKafka {

	public static final String ZOOKEEPER_CONNECTION = "192.168.0.101:2181";
	
    @BeforeClass
    public static void configureLogging() {
    	System.setProperty("org.slf4j.simpleLogger.log.kafka", "INFO");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.processors.kafka", "INFO");
        BasicConfigurator.configure();
    }
    
    @Test
    public void testIntegrationLocally() {
        final TestRunner runner = TestRunners.newTestRunner(GetKafka.class);
        runner.setProperty(GetKafka.ZOOKEEPER_CONNECTION_STRING, ZOOKEEPER_CONNECTION);
        runner.setProperty(GetKafka.TOPIC, "testX");
        runner.setProperty(GetKafka.KAFKA_TIMEOUT, "3 secs");
        runner.setProperty(GetKafka.ZOOKEEPER_TIMEOUT, "3 secs");
        
        runner.run(20, false);
        
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(GetKafka.REL_SUCCESS);
        for ( final MockFlowFile flowFile : flowFiles ) {
        	System.out.println(flowFile.getAttributes());
        	System.out.println(new String(flowFile.toByteArray()));
        	System.out.println();
        }
    }
    
}
