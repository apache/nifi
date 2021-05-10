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

import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;


public class BatchSize {
    private Integer count;
    private String size;
    private String duration;

    @ApiModelProperty("Preferred number of flow files to include in a transaction.")
    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @ApiModelProperty("Preferred number of bytes to include in a transaction.")
    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }

    @ApiModelProperty("Preferred amount of time that a transaction should span.")
    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, duration, size);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final BatchSize other = (BatchSize) obj;
        return Objects.equals(count, other.count) && Objects.equals(size, other.size) && Objects.equals(duration, other.duration);
    }
}
