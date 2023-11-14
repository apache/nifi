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
package org.apache.nifi.processors.slack.publish;

import com.slack.api.model.File;

import static org.apache.nifi.processors.slack.PublishSlack.linkify;
import static org.apache.nifi.util.StringUtils.isEmpty;

public class UploadLinkStrategy extends AbstractStrategy {

    @Override
    public boolean willSendContent() {
        return true;
    }

    @Override
    public boolean willUploadFile() {
        return true;
    }

    @Override
    public boolean willPostMessage() {
        return true;
    }

    @Override
    protected String getMessageText(PublishConfig config) {
        String text = config.getText();
        File upload = config.getUpload();
        String link = upload == null ? null : upload.getPermalink();
        String title = upload == null ? null : upload.getTitle();
        String linked = linkify(link, title);
        if (isEmpty(text)) {
            return linked;
        }
        if (isEmpty(linked)) {
            return text;
        }
        return text + "\n" + linked;
    }

}
