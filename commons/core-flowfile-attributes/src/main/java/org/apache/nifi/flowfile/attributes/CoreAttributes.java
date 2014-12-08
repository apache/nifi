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
package org.apache.nifi.flowfile.attributes;

public enum CoreAttributes implements FlowFileAttributeKey {
    /**
     * The flowfile's path indicates the relative directory to which a FlowFile belongs and does not
     * contain the filename
     */
    PATH("path"),
    
    /**
     * The flowfile's absolute path indicates the absolute directory to which a FlowFile belongs and does not
     * contain the filename
     */
    ABSOLUTE_PATH("absolute.path"),
    
    /**
     * The filename of the FlowFile. The filename should not contain any directory structure.
     */
    FILENAME("filename"),
    
    /**
     * A unique UUID assigned to this FlowFile
     */
    UUID("uuid"),
    
    /**
     * A numeric value indicating the FlowFile priority
     */
    PRIORITY("priority"),
    
    /**
     * The MIME Type of this FlowFile
     */
    MIME_TYPE("mime.type"),
    
    /**
     * Specifies the reason that a FlowFile is being discarded
     */
    DISCARD_REASON("discard.reason"),

    /**
     * Indicates an identifier other than the FlowFile's UUID that is known to refer to this FlowFile.
     */
    ALTERNATE_IDENTIFIER("alternate.identifier");
    
    private final String key;
    private CoreAttributes(final String key) {
        this.key = key;
    }
    
    @Override
    public String key() {
        return key;
    }

}
