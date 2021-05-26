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
import org.apache.nifi.registry.extension.bundle.BundleType;
import org.apache.nifi.registry.extension.component.manifest.ExtensionType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Parameters for filtering on extensions. All parameters will be AND'd together, but tags will be OR'd.
 */
@ApiModel
public class ExtensionFilterParams {

    private final BundleType bundleType;
    private final ExtensionType extensionType;
    private final Set<String> tags;

    // Used by Jackson
    private ExtensionFilterParams() {
        bundleType = null;
        extensionType = null;
        tags = null;
    }

    private ExtensionFilterParams(final Builder builder) {
        this.bundleType = builder.bundleType;
        this.extensionType = builder.extensionType;
        this.tags = Collections.unmodifiableSet(new HashSet<>(builder.tags));
    }

    @ApiModelProperty("The type of bundle")
    public BundleType getBundleType() {
        return bundleType;
    }

    @ApiModelProperty("The type of extension")
    public ExtensionType getExtensionType() {
        return extensionType;
    }

    @ApiModelProperty("The tags")
    public Set<String> getTags() {
        return tags;
    }

    public static class Builder {

        private BundleType bundleType;
        private ExtensionType extensionType;
        private Set<String> tags = new HashSet<>();

        public Builder bundleType(final BundleType bundleType) {
            this.bundleType = bundleType;
            return this;
        }

        public Builder extensionType(final ExtensionType extensionType) {
            this.extensionType = extensionType;
            return this;
        }

        public Builder tag(final String tag) {
            if (tag != null) {
                tags.add(tag);
            }
            return this;
        }

        public Builder addTags(final Collection<String> tags) {
            if (tags != null) {
                this.tags.addAll(tags);
            }
            return this;
        }

        public ExtensionFilterParams build() {
            return new ExtensionFilterParams(this);
        }
    }
}
