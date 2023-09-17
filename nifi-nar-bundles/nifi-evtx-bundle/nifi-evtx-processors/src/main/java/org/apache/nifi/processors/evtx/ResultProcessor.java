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

package org.apache.nifi.processors.evtx;

import com.google.common.net.MediaType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

public class ResultProcessor {
    private final Relationship successRelationship;
    private final Relationship failureRelationship;
    public static final String UNABLE_TO_PROCESS_DUE_TO = "Unable to process {} due to {}";

    public ResultProcessor(Relationship successRelationship, Relationship failureRelationship) {
        this.successRelationship = successRelationship;
        this.failureRelationship = failureRelationship;
    }

    public void process(ProcessSession session, ComponentLog logger, FlowFile updated, Exception exception, String name) {
        updated = session.putAttribute(updated, CoreAttributes.FILENAME.key(), name);
        updated = session.putAttribute(updated, CoreAttributes.MIME_TYPE.key(), MediaType.APPLICATION_XML_UTF_8.toString());
        if (exception == null) {
            session.transfer(updated, successRelationship);
        } else {
            logger.error(UNABLE_TO_PROCESS_DUE_TO, name, exception, exception);
            session.transfer(updated, failureRelationship);
        }
    }
}
