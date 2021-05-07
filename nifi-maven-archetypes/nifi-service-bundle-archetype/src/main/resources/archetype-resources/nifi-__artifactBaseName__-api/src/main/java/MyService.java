package ${package};

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"example"})
@CapabilityDescription("Example Service API.")
public interface MyService extends ControllerService {

    public void execute()  throws ProcessException;

}
