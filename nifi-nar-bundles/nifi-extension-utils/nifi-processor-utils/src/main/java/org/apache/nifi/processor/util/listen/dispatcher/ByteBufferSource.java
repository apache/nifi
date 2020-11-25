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
package org.apache.nifi.processor.util.listen.dispatcher;

import java.nio.ByteBuffer;

/**
 * Manages byte buffers for the dispatchers.
 */
public interface ByteBufferSource {

    /**
     * @return Returns a buffer for usage. The buffer can be pooled or created on demand depending on the implementation.
     * If the source is not capable to provide an instance, it returns {@code null} instead.
     */
    ByteBuffer acquire();

    /**
     * With calling this method the client releases the buffer. It might be reused by the handler and not to be used
     * by this client any more.
     *
     * @param byteBuffer The byte buffer the client acquired previously.
     */
    void release(ByteBuffer byteBuffer);
}
