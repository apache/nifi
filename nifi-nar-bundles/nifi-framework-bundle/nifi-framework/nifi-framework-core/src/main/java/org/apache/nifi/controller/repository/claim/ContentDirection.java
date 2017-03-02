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
package org.apache.nifi.controller.repository.claim;

/**
 * Specifies one side of the Provenance Event for which the Content Claim is being referenced
 */
public enum ContentDirection {

    /**
     * Indicates the Content Claim that was the Input to the Process that generating a Provenance Event
     */
    INPUT,
    /**
     * Indicates the Content Claim that is the Output of the process that generated the Provenance Event.
     */
    OUTPUT;
}
