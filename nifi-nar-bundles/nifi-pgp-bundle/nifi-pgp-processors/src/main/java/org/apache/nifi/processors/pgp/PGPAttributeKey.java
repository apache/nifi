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
package org.apache.nifi.processors.pgp;

/**
 * Pretty Good Privacy Attribute Key
 */
class PGPAttributeKey {
    static final String LITERAL_DATA_FILENAME = "pgp.literal.data.filename";

    static final String LITERAL_DATA_MODIFIED = "pgp.literal.data.modified";

    static final String SYMMETRIC_KEY_ALGORITHM = "pgp.symmetric.key.algorithm";

    static final String SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER = "pgp.symmetric.key.algorithm.block.cipher";

    static final String SYMMETRIC_KEY_ALGORITHM_KEY_SIZE = "pgp.symmetric.key.algorithm.key.size";

    static final String SYMMETRIC_KEY_ALGORITHM_ID = "pgp.symmetric.key.algorithm.id";

    static final String FILE_ENCODING = "pgp.file.encoding";

    static final String COMPRESS_ALGORITHM = "pgp.compression.algorithm";

    static final String COMPRESS_ALGORITHM_ID = "pgp.compression.algorithm.id";
}
