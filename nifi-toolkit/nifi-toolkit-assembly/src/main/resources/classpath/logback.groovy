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
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.status.NopStatusListener

statusListener(NopStatusListener)

appender('stdout', ConsoleAppender) {
    target = 'System.out'
    encoder(PatternLayoutEncoder) {
        pattern = "%date %level [%thread] %logger{40} %msg%n"
    }
}

appender('stderr', ConsoleAppender) {
    target = 'System.err'
    encoder(PatternLayoutEncoder) {
        pattern = "%date %level [%thread] %logger{40} %msg%n"
    }
}

logger("org.apache.nifi.toolkit.zkmigrator", INFO)
logger("org.apache.zookeeper", WARN)
root(WARN, ['stderr'])
