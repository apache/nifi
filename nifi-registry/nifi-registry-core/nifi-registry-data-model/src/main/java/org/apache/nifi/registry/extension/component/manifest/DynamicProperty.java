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
package org.apache.nifi.registry.extension.component.manifest;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

@ApiModel
@XmlAccessorType(XmlAccessType.FIELD)
public class DynamicProperty {

    private String name;
    private String value;
    private String description;
    private ExpressionLanguageScope expressionLanguageScope;
    private boolean expressionLanguageSupported;

    @ApiModelProperty(value = "The description of the dynamic property name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty(value = "The description of the dynamic property value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @ApiModelProperty(value = "The description of the dynamic property")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "Whether or not expression language is supported")
    public boolean isExpressionLanguageSupported() {
        return expressionLanguageSupported;
    }

    public void setExpressionLanguageSupported(boolean expressionLanguageSupported) {
        this.expressionLanguageSupported = expressionLanguageSupported;
    }

    @ApiModelProperty(value = "The scope of the expression language support")
    public ExpressionLanguageScope getExpressionLanguageScope() {
        return expressionLanguageScope;
    }

    public void setExpressionLanguageScope(ExpressionLanguageScope expressionLanguageScope) {
        this.expressionLanguageScope = expressionLanguageScope;
    }
}
