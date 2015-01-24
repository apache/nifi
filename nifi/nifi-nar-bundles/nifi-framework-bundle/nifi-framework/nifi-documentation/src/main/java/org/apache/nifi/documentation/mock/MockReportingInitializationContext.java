package org.apache.nifi.documentation.mock;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.ControllerServiceLookup;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.scheduling.SchedulingStrategy;

public class MockReportingInitializationContext implements ReportingInitializationContext {

	@Override
	public String getIdentifier() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getSchedulingPeriod(TimeUnit timeUnit) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ControllerServiceLookup getControllerServiceLookup() {
		return new MockControllerServiceLookup();
	}

	@Override
	public String getSchedulingPeriod() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SchedulingStrategy getSchedulingStrategy() {
		// TODO Auto-generated method stub
		return null;
	}

}
