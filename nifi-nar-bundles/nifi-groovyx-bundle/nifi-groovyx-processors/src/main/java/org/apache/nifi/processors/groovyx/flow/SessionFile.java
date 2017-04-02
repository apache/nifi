package org.apache.nifi.processors.groovyx.flow;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

// import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;

/**
 * The Flow file implementation that contains reference to the session. 
 * So all commands become easier. Example:
 * <code>flowFile.putAttribute("AttrName", "AttrValue");</code>
 */
//CHECKSTYLE:OFF
@SuppressWarnings("PMD")
public class SessionFile implements FlowFile{

    FlowFile flowFile;
    ProcessSessionWrap session;

    protected SessionFile(ProcessSessionWrap session, FlowFile f) {
        if (f == null || session == null) {
            throw new NullPointerException("Session and FlowFile are mandatory session=" + session + " file=" + f);
        }
        if (f instanceof SessionFile) {
            throw new RuntimeException("file could be instanceof SessionFile");
        }
        this.flowFile = f;
        this.session = session;
    }

    public FlowFile unwrap() {
        return flowFile;
    }
    public ProcessSessionWrap session() {
        return session;
    }

    public SessionFile clone(boolean cloneContent) {
        if (cloneContent) {
            return new SessionFile(session, session.clone(flowFile));
        }
        return new SessionFile(session, session.create(flowFile));
    }

    public InputStream read() {
        return session.read(flowFile);
    }

    public void write(StreamCallback c) {
        session.write(this, c);
    }

    public void write(OutputStreamCallback c) {
        session.write(this, c);
    }


    public void putAttribute(String key, String value) {
        session.putAttribute(this, key, value);
    }

    public void putAllAttributes(Map m) {
        session.putAllAttributes(this, m);
    }
    
    public void removeAttribute(String key) {
        session.removeAttribute(this, key);
    }
    
    public void removeAllAttributes(Collection<String> keys) {
        Set<String> keyset = (Set<String>) (keys instanceof Set ? keys : new HashSet(keys));
        session.removeAllAttributes(this, keyset);
    }

    public void transfer(Relationship r) {
        session.transfer(this, r);
    }

    public void remove() {
        session.remove(this);
    }
    //OVERRIDE
    @Override
    public long getId(){
    	return flowFile.getId();
    }
    @Override
    public long getEntryDate(){
    	return flowFile.getEntryDate();
    }
    @Override
    public long getLineageStartDate(){
    	return flowFile.getLineageStartDate();
    }
    @Override
    public long getLineageStartIndex(){
    	return flowFile.getLineageStartIndex();
    }
    @Override
    public Long getLastQueueDate(){
    	return flowFile.getLastQueueDate();
    }
    @Override
    public long getQueueDateIndex(){
    	return flowFile.getQueueDateIndex();
    }
    @Override
    public boolean isPenalized(){
    	return flowFile.isPenalized();
    }
    @Override
    public String getAttribute(String key) {
        return flowFile.getAttribute(key);
    }
    @Override
    public long getSize(){
    	return flowFile.getSize();
    }
    /**
     * @return an unmodifiable map of the flow file attributes
     */
    @Override
    public Map<String, String> getAttributes() {
        return flowFile.getAttributes();
    }
    
    public int compareTo(FlowFile other){
    	if(other instanceof SessionFile){
    		other=((SessionFile)other).flowFile;
    	}
    	return flowFile.compareTo(other);
    }


    @Override
    public String toString() {
        return "WRAP[" + flowFile.toString() + "]";
    }

}
