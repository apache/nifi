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
package org.apache.nifi.registry.extension.component;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.SortedSet;

@ApiModel
public class ExtensionMetadataContainer {

    private int numResults;
    private ExtensionFilterParams filterParams;
    private SortedSet<ExtensionMetadata> extensions;

    @ApiModelProperty("The number of extensions in the response")
    public int getNumResults() {
        return numResults;
    }

    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }

    @ApiModelProperty("The filter parameters submitted for the request")
    public ExtensionFilterParams getFilterParams() {
        return filterParams;
    }

    public void setFilterParams(ExtensionFilterParams filterParams) {
        this.filterParams = filterParams;
    }

    @ApiModelProperty("The metadata for the extensions")
    public SortedSet<ExtensionMetadata> getExtensions() {
        return extensions;
    }

    public void setExtensions(SortedSet<ExtensionMetadata> extensions) {
        this.extensions = extensions;
    }
}
