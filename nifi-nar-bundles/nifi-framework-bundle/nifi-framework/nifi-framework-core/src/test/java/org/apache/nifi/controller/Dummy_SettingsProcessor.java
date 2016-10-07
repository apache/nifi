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
@DefaultSettings(YieldDuration = "5 sec", PenaltyDuration = "1 min", LogLevel = LogLevel.DEBUG)
public class Dummy_SettingsProcessor extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
