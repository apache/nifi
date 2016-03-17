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
package org.apache.nifi.processors.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

public interface SequenceFileWriter {

    /**
     * Creates a Sequence File by writing the given FlowFile as key/value pairs. The provided FlowFile may be a package of multiple FlowFiles, or just one. The keys for the Sequence File are the flow
     * files' logical names. The values are the flow files' content.
     *
     * @param flowFile - the FlowFile to write to the Sequence File.
     * @param session session
     * @param configuration configuration
     * @param compressionType compression type
     * @return the written to SequenceFile flow file
     */
    FlowFile writeSequenceFile(FlowFile flowFile, ProcessSession session, Configuration configuration, CompressionType compressionType);
}
