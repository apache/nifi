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
package org.apache.nifi.services.slack;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.slf4j.Logger;

import java.util.Date;

public class SlackPostMessageResponse {
    private Boolean ok;
    private String channel;
    @JsonDeserialize(using = TimestampDeserializer.class)
    private Date ts;
    private Message message;
    private String error;
    private String warning;

    public Boolean isOk() {
        return ok;
    }

    public void setOk(Boolean ok) {
        this.ok = ok;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getWarning() {
        return warning;
    }

    public void setWarning(String warning) {
        this.warning = warning;
    }

    public void checkResponse(final Logger logger) throws SlackRestServiceException {
        if (isOk() == null) {
            throw new SlackRestServiceException("Slack response JSON does not contain 'ok' key or it has invalid value.");
        }
        if (!isOk()) {
            throw new SlackRestServiceException("Slack error response: " + getError());
        }

        if (getWarning() != null) {
            logger.warn("Slack warning message: " + getWarning());
        }
    }
}
