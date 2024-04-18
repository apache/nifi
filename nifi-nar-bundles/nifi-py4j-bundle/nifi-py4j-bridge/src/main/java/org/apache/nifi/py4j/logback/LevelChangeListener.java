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
package org.apache.nifi.py4j.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggerContextListener;
import org.apache.nifi.py4j.logging.LogLevelChangeListener;
import org.apache.nifi.logging.LogLevel;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Logback Listener implementation for tracking Logger Level changes and notifying framework listeners
 */
public class LevelChangeListener implements LoggerContextListener {
    private static final Map<Level, LogLevel> LEVELS = Map.of(
            Level.ALL, LogLevel.TRACE,
            Level.TRACE, LogLevel.TRACE,
            Level.DEBUG, LogLevel.DEBUG,
            Level.INFO, LogLevel.INFO,
            Level.WARN, LogLevel.WARN,
            Level.ERROR, LogLevel.ERROR,
            Level.OFF, LogLevel.NONE
    );

    private static final List<Pattern> EXCLUDED_LOGGERS = List.of(
            Pattern.compile("^$"),
            Pattern.compile("^org|com|net$"),
            Pattern.compile("^jetbrains.*"),
            Pattern.compile("^org\\.apache.*"),
            Pattern.compile("^org\\.eclipse.*"),
            Pattern.compile("^org\\.glassfish.*"),
            Pattern.compile("^org\\.springframework.*"),
            Pattern.compile("^org\\.opensaml.*"),
            Pattern.compile("^software\\.amazon.*")
    );

    private final LogLevelChangeListener logLevelChangeListener;

    LevelChangeListener(final LogLevelChangeListener logLevelChangeListener) {
        this.logLevelChangeListener = Objects.requireNonNull(logLevelChangeListener, "Listener required");
    }

    /**
     * Register an instance of Logback LevelChangeListener routing to framework LogLevelChangeListener
     */
    public static void registerLogbackListener(final LogLevelChangeListener logLevelChangeListener) {
        final LevelChangeListener levelChangeListener = new LevelChangeListener(logLevelChangeListener);
        final ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
        if (loggerFactory instanceof LoggerContext loggerContext) {
            loggerContext.addListener(levelChangeListener);
            levelChangeListener.onStart(loggerContext);
        }
    }

    /**
     * Reset Resistant enabled to support persistent behavior when the LoggerContext is changed
     *
     * @return Reset Resistant enabled
     */
    @Override
    public boolean isResetResistant() {
        return true;
    }

    /**
     * Set initial Logger Levels from registered loggers when starting
     *
     * @param loggerContext Logback Context
     */
    @Override
    public void onStart(final LoggerContext loggerContext) {
        for (final Logger logger : loggerContext.getLoggerList()) {
            onLevelChange(logger, logger.getEffectiveLevel());
        }
    }

    /**
     * Set Logger Levels from registered loggers when resetting
     *
     * @param loggerContext Logback Context
     */
    @Override
    public void onReset(final LoggerContext loggerContext) {
        onStart(loggerContext);
    }

    @Override
    public void onStop(final LoggerContext loggerContext) {

    }

    /**
     * Send logger level changes to framework handler for subsequent notification
     *
     * @param logger Logback Logger with new level
     * @param level New level assigned for Logback Logger
     */
    @Override
    public void onLevelChange(final Logger logger, final Level level) {
        Objects.requireNonNull(level, "Level required");

        final String loggerName = logger.getName();
        if (isLoggerSupported(loggerName)) {
            final LogLevel logLevel = LEVELS.getOrDefault(level, LogLevel.WARN);
            logLevelChangeListener.onLevelChange(loggerName, logLevel);
        }
    }

    private boolean isLoggerSupported(final String loggerName) {
        boolean supported = true;

        for (final Pattern loggerPattern : EXCLUDED_LOGGERS) {
            final Matcher matcher = loggerPattern.matcher(loggerName);
            if (matcher.matches()) {
                supported = false;
                break;
            }
        }

        return supported;
    }
}
