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
package org.apache.nifi.logging;


/**
 * The ProcessorLog is an extension of ComponentLog but provides no additional functionality.
 * It exists because ProcessorLog was created first,
 * but when Controller Services and Reporting Tasks began to be used more heavily loggers
 * were needed for them as well. We did not want to return a ProcessorLog to a ControllerService
 * or a ReportingTask, so all of the methods were moved to a higher interface named ComponentLog.
 * However, we kept the ProcessorLog interface around in order to maintain backward compatibility.
 */
public interface ProcessorLog extends ComponentLog {

}
