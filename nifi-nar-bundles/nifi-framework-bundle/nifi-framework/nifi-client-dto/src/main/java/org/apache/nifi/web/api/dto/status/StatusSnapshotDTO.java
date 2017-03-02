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

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Date;
import java.util.Map;
import javax.xml.bind.annotation.XmlType;

/**
 * A snapshot of the status at a given time.
 */
@XmlType(name = "statusSnapshot")
public class StatusSnapshotDTO {

    private Date timestamp;
    private Map<String, Long> statusMetrics;

    /**
     * @return timestamp of this snapshot
     */
    @ApiModelProperty(
            value = "The timestamp of the snapshot."
    )
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return status metrics
     */
    @ApiModelProperty(
            value = "The status metrics."
    )
    public Map<String, Long> getStatusMetrics() {
        return statusMetrics;
    }

    public void setStatusMetrics(Map<String, Long> statusMetrics) {
        this.statusMetrics = statusMetrics;
    }

}
