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
package org.apache.nifi.xml.processing;

import javax.xml.XMLConstants;

/**
 * XML Processing Attributes
 */
public enum ProcessingAttribute {
    /** Access External Document Type Declaration with an empty string to deny all access to external references */
    ACCESS_EXTERNAL_DTD(XMLConstants.ACCESS_EXTERNAL_DTD, ""),

    /** Access External Stylesheet with an empty string to deny all access to external references */
    ACCESS_EXTERNAL_STYLESHEET(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");

    private final String attribute;

    private final Object value;

    ProcessingAttribute(final String attribute, final Object value) {
        this.attribute = attribute;
        this.value = value;
    }

    public String getAttribute() {
        return attribute;
    }

    public Object getValue() {
        return value;
    }
}
