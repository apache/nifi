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
package org.apache.nifi.toolkit.kafkamigrator.descriptor;

import java.util.HashMap;
import java.util.Map;

public class FlowPropertyXpathDescriptor implements PropertyXpathDescriptor {

    private static final Map<String, String> CONSUME_TRANSACTION_PROPERTIES;
    private static final Map<String, String> PUBLISH_TRANSACTION_PROPERTIES;
    private static final Map<KafkaProcessorType, Map<String, String>> TRANSACTION_PROPERTIES;
    static {
        CONSUME_TRANSACTION_PROPERTIES = new HashMap<>();
        CONSUME_TRANSACTION_PROPERTIES.put("xpathForTransactionProperty", "property[name=\"honor-transactions\"]/value");
        CONSUME_TRANSACTION_PROPERTIES.put("transactionTagName", "honor-transactions");
        PUBLISH_TRANSACTION_PROPERTIES = new HashMap<>();
        PUBLISH_TRANSACTION_PROPERTIES.put("xpathForTransactionProperty", "property[name=\"use-transactions\"]/value");
        PUBLISH_TRANSACTION_PROPERTIES.put("transactionTagName", "use-transactions");
        TRANSACTION_PROPERTIES = new HashMap<>();
        TRANSACTION_PROPERTIES.put(KafkaProcessorType.CONSUME, CONSUME_TRANSACTION_PROPERTIES);
        TRANSACTION_PROPERTIES.put(KafkaProcessorType.PUBLISH, PUBLISH_TRANSACTION_PROPERTIES);
    }

    private final KafkaProcessorType processorType;

    public FlowPropertyXpathDescriptor(final KafkaProcessorType processorType) {
        this.processorType = processorType;
    }

    @Override
    public String getXpathForProperties() {
        return "property";
    }

    @Override
    public String getPropertyKeyTagName() {
        return "name";
    }

    @Override
    public String getPropertyTagName() {
        return "property";
    }

    @Override
    public String getXpathForTransactionProperty() {
        return TRANSACTION_PROPERTIES.get(processorType).get("xpathForTransactionProperty");
    }

    @Override
    public String getTransactionTagName() {
        return TRANSACTION_PROPERTIES.get(processorType).get("transactionTagName");
    }
}
