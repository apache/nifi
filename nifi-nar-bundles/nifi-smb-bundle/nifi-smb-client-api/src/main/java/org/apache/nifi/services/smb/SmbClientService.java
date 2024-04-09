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
package org.apache.nifi.services.smb;

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

/**
 * Service abstraction for Server Message Block protocol operations.
 */
public interface SmbClientService extends AutoCloseable {

    Stream<SmbListableEntity> listFiles(String directoryPath);

    void ensureDirectory(String directoryPath);

    void readFile(String filePath, OutputStream outputStream) throws IOException;

    void moveFile(String filePath, String directoryPath);

    void deleteFile(String filePath);
}
