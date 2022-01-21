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
package org.apache.nifi.flow.resource;

/**
 * The external resource provider service is an internal NiFi service which is responsible to gather files needed for the
 * healthy behaviour of the flow. These files are called external resources and might serve various purposes: the given files
 * might be used database driver, configuration file for processors, etc. The provider service is not responsible for the
 * proper usage of the external resources, it merely collects them.
 *
 * The service might gather these external resources from various external sources using different strategies to fetch them
 * depending on the setup. This interface serves as a facade in front of the working parts, exposing only the necessary
 * interactions.
 */
public interface ExternalResourceProviderService {

    /**
     * Instructs the service to start polling for external resources and acquire them if predefined conditions apply.
     */
    void start();

    /**
     * Instructs the service to stop polling. All files already polled remain.
     */
    void stop();
}
