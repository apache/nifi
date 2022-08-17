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
package org.apache.nifi.registry.provider.extension;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.extension.BundlePersistenceContext;
import org.apache.nifi.registry.extension.BundleVersionCoordinate;

public class StandardBundlePersistenceContext implements BundlePersistenceContext {

    private final BundleVersionCoordinate coordinate;
    private final String author;
    private final long timestamp;
    private final long bundleSize;

    private StandardBundlePersistenceContext(final Builder builder) {
        this.coordinate = builder.coordinate;
        this.bundleSize = builder.bundleSize;
        this.author = builder.author;
        this.timestamp = builder.timestamp;
        Validate.notNull(this.coordinate);
        Validate.notBlank(this.author);
    }


    @Override
    public BundleVersionCoordinate getCoordinate() {
        return coordinate;
    }

    @Override
    public long getSize() {
        return bundleSize;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String getAuthor() {
        return author;
    }

    public static class Builder {

        private BundleVersionCoordinate coordinate;
        private String author;
        private long timestamp;
        private long bundleSize;

        public Builder coordinate(final BundleVersionCoordinate identifier) {
            this.coordinate = identifier;
            return this;
        }

        public Builder author(final String author) {
            this.author = author;
            return this;
        }

        public Builder timestamp(final long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder bundleSize(final long size) {
            this.bundleSize = size;
            return this;
        }

        public StandardBundlePersistenceContext build() {
            return new StandardBundlePersistenceContext(this);
        }

    }

}
