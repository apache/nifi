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
package org.apache.nifi.extension;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Comparator;
import java.util.Objects;

public class TagCount implements Comparable<TagCount> {

    private String tag;
    private int count;

    @Schema(description = "The tag label")
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Schema(description = "The number of occurrences of the given tag")
    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public int compareTo(TagCount o) {
        return Comparator.comparing(TagCount::getTag).compare(this, o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagCount tagCount = (TagCount) o;
        return count == tagCount.count && Objects.equals(tag, tagCount.tag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, count);
    }
}
