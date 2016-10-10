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
package org.apache.nifi.processors.flume.util;


public class FlowFileEventConstants {

  // FlowFile#getEntryDate();
  public static final String ENTRY_DATE_HEADER = "nifi.entry.date";

  // FlowFile#getId();
  public static final String ID_HEADER = "nifi.id";

  // FlowFile#getLastQueueDate();
  public static final String LAST_QUEUE_DATE_HEADER = "nifi.last.queue.date";

  // FlowFile#getLineageStartDate();
  public static final String LINEAGE_START_DATE_HEADER = "nifi.lineage.start.date";

  // FlowFile#getSize();
  public static final String SIZE_HEADER = "nifi.size";

}
