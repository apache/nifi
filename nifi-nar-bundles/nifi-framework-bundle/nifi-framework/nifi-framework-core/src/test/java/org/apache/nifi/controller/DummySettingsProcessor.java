package org.apache.nifi.controller;

import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Dummy Processor to test @DefaultSettings annotation
 */
@DefaultSettings(yieldDuration = "5 sec", penaltyDuration = "1 min", logLevel = LogLevel.DEBUG)
public class DummySettingsProcessor extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
