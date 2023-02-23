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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Date;

public class Message {
    private String text;
    private String username;
    @JsonProperty("first_name")
    private String botId;
    @JsonProperty("attachments")
    private Attachment[] attachment;
    private String type;
    private String subtype;
    @JsonDeserialize(using = TimestampDeserializer.class)
    private Date ts;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getBotId() {
        return botId;
    }

    public void setBotId(String botId) {
        this.botId = botId;
    }

    public Attachment[] getAttachment() {
        return attachment;
    }

    public void setAttachment(Attachment[] attachment) {
        this.attachment = attachment;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSubtype() {
        return subtype;
    }

    public void setSubtype(String subtype) {
        this.subtype = subtype;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }
}
