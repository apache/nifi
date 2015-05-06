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
package org.apache.nifi.processor.exception;

/**
 * Thrown to indicate that the content for some FlowFile could not be found.
 * This indicates that the data is gone and likely cannot be restored and any
 * information about the FlowFile should be discarded entirely. This likely
 * indicates some influence from an external process outside the control of the
 * framework and it is not something any processor or the framework can recover
 * so it must discard the object.
 *
 */
public class MissingFlowFileException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MissingFlowFileException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
