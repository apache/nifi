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
package org.apache.nifi.processors.hive;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.hive.HiveDBCPService;
import org.apache.nifi.processor.AbstractProcessor;

/**
 * An abstract base class for HiveQL processors to share common data, methods, etc.
 */
public abstract class AbstractHiveQLProcessor extends AbstractProcessor {

    public static final PropertyDescriptor HIVE_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Hive Database Connection Pooling Service")
            .description("The Hive Controller Service that is used to obtain connection(s) to the Hive database")
            .required(true)
            .identifiesControllerService(HiveDBCPService.class)
            .build();

}
