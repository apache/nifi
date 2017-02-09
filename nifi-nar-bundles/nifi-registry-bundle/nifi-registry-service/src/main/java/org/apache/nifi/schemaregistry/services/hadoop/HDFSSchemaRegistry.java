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

package org.apache.nifi.schemaregistry.services.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.util.StopWatch;

@Tags({ "schema", "registry", "avro", "json", "csv" })
@CapabilityDescription("Provides a service for registering and accessing schemas stored in HDFS. You can register schema "
        + "as a dynamic property where 'name' represents the schema name.")
public final class HDFSSchemaRegistry extends AbstractHadoopControllerService implements SchemaRegistry {

    @Override
    public String retrieveSchemaText(String schemaName) {
        final FileSystem hdfs = getFileSystem();
        final String filenameValue = schemaName;

        Path path = null;
        try {
            path = new Path(schemaDirectory + schemaName);
        } catch (IllegalArgumentException e) {
            getLogger().error("Failed to retrieve Schema from {}  due to {}", new Object[] {filenameValue, e});
            throw e;
        }

        BufferedReader stream = null;

        final URI uri = path.toUri();
        final StopWatch stopWatch = new StopWatch(true);
        StringBuilder schema = new StringBuilder();
        try {
            stream = new BufferedReader(new InputStreamReader(hdfs.open(path, 16384)));
            stream.lines().forEach(schema::append);

            stopWatch.stop();
            getLogger().info("Successfully received schema from {} in {}", new Object[] {uri, stopWatch.getDuration()});

        } catch (final IOException e) {
            getLogger().error("Failed to retrieve schema from {} due to {}", new Object[] {uri, e});
            throw new IllegalArgumentException(e);
        } finally {
            IOUtils.closeQuietly(stream);
        }

        return schema.toString();
    }

    @Override
    public String retrieveSchemaText(String schemaName, Properties attributes) {
        throw new UnsupportedOperationException("This version of schema registry does not "
                + "support this operation, since schemas are only identified by name.");
    }

    @Override
    @OnDisabled
    public void close() throws Exception {
        // no op
    }
}
