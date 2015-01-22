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
package org.apache.nifi.web.api.dto.status;

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing a status descriptor.
 */
@XmlType(name = "statusDescriptor")
public class StatusDescriptorDTO {

    public enum Formatter {

        COUNT,
        DURATION,
        DATA_SIZE
    };

    private String field;
    private String label;
    private String description;
    private String formatter;

    public StatusDescriptorDTO() {
    }

    public StatusDescriptorDTO(final String field, final String label, final String description, final String formatter) {
        this.field = field;
        this.label = label;
        this.description = description;
        this.formatter = formatter;
    }

    /**
     * The name of this status field.
     *
     * @return
     */
    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    /**
     * The label of this status field.
     *
     * @return
     */
    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * The description of this status field.
     *
     * @return
     */
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * The formatter for this descriptor.
     *
     * @return
     */
    public String getFormatter() {
        return formatter;
    }

    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }

}
