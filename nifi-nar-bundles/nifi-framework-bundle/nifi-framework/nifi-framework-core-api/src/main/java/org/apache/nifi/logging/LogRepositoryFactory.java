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

import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
public class LogRepositoryFactory {

    public static final String LOG_REPOSITORY_CLASS_NAME = "org.apache.nifi.logging.repository.StandardLogRepository";

    private static final ConcurrentMap<String, LogRepository> repositoryMap = new ConcurrentHashMap<>();
    private static final Class<LogRepository> logRepositoryClass;

    static {
        Class<LogRepository> clazz = null;
        try {
            clazz = (Class<LogRepository>) Class.forName(LOG_REPOSITORY_CLASS_NAME, true, LogRepositoryFactory.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            LoggerFactory.getLogger(LogRepositoryFactory.class).error("Unable to find class {}; logging may not work properly", LOG_REPOSITORY_CLASS_NAME);
        }
        logRepositoryClass = clazz;
    }

    public static LogRepository getRepository(final String componentId) {
        LogRepository repository = repositoryMap.get(requireNonNull(componentId));
        if (repository == null) {
            try {
                repository = logRepositoryClass.newInstance();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }

            final LogRepository oldRepository = repositoryMap.putIfAbsent(componentId, repository);
            if (oldRepository != null) {
                repository = oldRepository;
            }
        }

        return repository;
    }

    public static LogRepository removeRepository(final String componentId) {
        return repositoryMap.remove(requireNonNull(componentId));
    }
}
