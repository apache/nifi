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

package org.apache.nifi.web.standard.api.transformjson.dto;

import java.io.Serializable;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class JoltSpecificationDTO implements Serializable{


    private String transform;
    private String specification;
    private String input;
    private String modules;
    private String customClass;
    private Map<String,String> expressionLanguageAttributes;

    public JoltSpecificationDTO() {
    }

    public JoltSpecificationDTO(String transform, String specification) {
        this.transform = transform;
        this.specification = specification;
    }

    public String getTransform() {
        return transform;
    }

    public void setTransform(String transform) {
        this.transform = transform;
    }

    public String getSpecification() {
        return specification;
    }

    public void setSpecification(String specification) {
        this.specification = specification;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getModules() {
        return modules;
    }

    public void setModules(String modules) {
        this.modules = modules;
    }

    public String getCustomClass() {
        return customClass;
    }

    public void setCustomClass(String customClass) {
        this.customClass = customClass;
    }

    public Map<String, String> getExpressionLanguageAttributes() {
        return expressionLanguageAttributes;
    }

    public void setExpressionLanguageAttributes(Map<String, String> expressionLanguageAttributes) {
        this.expressionLanguageAttributes = expressionLanguageAttributes;
    }
}
