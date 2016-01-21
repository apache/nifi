package org.apache.nifi.processors.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.StartupListener;
import org.apache.camel.impl.DefaultExchange;
import org.apache.nifi.logging.ProcessorLog;

public class CamelContextStartupListener implements StartupListener {

    private final ProcessorLog processLog;

    public CamelContextStartupListener(ProcessorLog logger) {
       this.processLog=logger;
    }

    @Override
    public void onCamelContextStarted(CamelContext context, boolean alreadyStarted) throws Exception {
       if(!alreadyStarted){
           processLog.info("Camel Spring Context Started");
       }    
      // context.createProducerTemplate().sendBody("direct:start", "Hello from Thread:"+Thread.currentThread().getId() +" Id:"+Thread.currentThread().getId());
    }
    
    

}
