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

package org.apache.nifi.registry.flow;

import java.util.Map;

import io.swagger.annotations.ApiModelProperty;

public class VersionedLabel extends VersionedComponent {
    private String label;

    private Double width;
    private Double height;

    private Map<String, String> style;


    @ApiModelProperty("The text that appears in the label.")
    public String getLabel() {
        return label;
    }

    public void setLabel(final String label) {
        this.label = label;
    }

    @ApiModelProperty("The styles for this label (font-size : 12px, background-color : #eee, etc).")
    public Map<String, String> getStyle() {
        return style;
    }

    public void setStyle(final Map<String, String> style) {
        this.style = style;
    }

    @ApiModelProperty("The height of the label in pixels when at a 1:1 scale.")
    public Double getHeight() {
        return height;
    }

    public void setHeight(Double height) {
        this.height = height;
    }

    @ApiModelProperty("The width of the label in pixels when at a 1:1 scale.")
    public Double getWidth() {
        return width;
    }

    public void setWidth(Double width) {
        this.width = width;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.LABEL;
    }
}
