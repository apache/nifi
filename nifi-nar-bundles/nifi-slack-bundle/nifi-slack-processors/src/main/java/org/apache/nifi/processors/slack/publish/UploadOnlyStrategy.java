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

import static org.apache.nifi.util.StringUtils.isEmpty;

public class UploadOnlyStrategy extends AbstractStrategy {

    @Override
    public boolean willSendContent() {
        return true;
    }

    @Override
    public boolean willUploadFile() {
        return true;
    }

    @Override
    protected String getUploadChannel(PublishConfig config) {
        if (isEmpty(config.getUploadChannel())) {
            // fallback to CHANNEL in case the user set the wrong Property
            return config.getChannel();
        }
        return config.getUploadChannel();
    }

    @Override
    protected String getInitialComment(PublishConfig config) {
        if (isEmpty(config.getInitComment())) {
            // fallback to TEXT in case the user set the wrong Property
            return config.getText();
        }
        return config.getInitComment();
    }
}
