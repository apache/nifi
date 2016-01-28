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
package org.apache.nifi.processors.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.StartupListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.logging.ProcessorLog;

public class CamelContextStartupListener implements StartupListener {

    private final ProcessorLog processLog;
    private final String grapeGrabURLs;

    public CamelContextStartupListener(final ProcessorLog logger, final String grapeGrabURLs) {
        this.processLog = logger;
        this.grapeGrabURLs=grapeGrabURLs;
    }

    @Override
    public void onCamelContextStarted(CamelContext context, boolean alreadyStarted) throws Exception {
        if (!alreadyStarted) {
            processLog.info("Camel Spring Context Started");
        }

        //Let's load Extra Libraries using grape those might not be present in classpath.
        if(!StringUtils.isEmpty(grapeGrabURLs)){
            for (String  grapeGrabURL : grapeGrabURLs.split(",")) {
                String [] gav=grapeGrabURL.split("/");
                context.createProducerTemplate()
                .sendBody("grape:grape", gav[0]+"/"+gav[1]+"/"+(gav[2].equalsIgnoreCase("default")?context.getVersion():gav[2]));
            }
        }
    }
}