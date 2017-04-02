package org.apache.nifi.processors.groovyx;

import groovy.lang.Closure;
import groovy.lang.DelegatingMetaClass;
import groovy.lang.GroovySystem;
import groovy.lang.MetaClass;
import groovy.lang.Writable;

import org.apache.nifi.processors.groovyx.flow.ProcessSessionWrap;
import org.apache.nifi.processors.groovyx.flow.SessionFile;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult;
//import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.OutputStream;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.io.InputStream;
import java.io.IOException;

/**
 * Class to initialize additional groovy methods to work with SessionFile, Relationship, and Sessions easier
 */
// CHECKSTYLE:OFF
@SuppressWarnings("PMD")
class GroovyMethods{
	private static boolean initialized = false;
	
	static void init(){
		if(!initialized){
			synchronized(GroovyMethods.class) {
				if(!initialized){
					initialized = metaSessionFile() && metaRelationship() && metaSession() && metaFlowFile();
					System.out.println ("Groovy extension initialized: "+initialized);
				}
			}
		}
	}
	
	static HashSet fileProps = new HashSet();
	static{
		fileProps.add("attributes");
		fileProps.add("size");
	}
	
	
	private static boolean metaSessionFile(){
		GroovySystem.getMetaClassRegistry().setMetaClass(SessionFile.class,new DelegatingMetaClass(SessionFile.class) {
			@Override
			public Object getProperty(Object object, String key) {
				if (!fileProps.contains(key) && object instanceof SessionFile) {
					SessionFile f = (SessionFile) object;
					return f.getAttribute(key);
				}
				return super.getProperty(object, key);
			}
			
			@Override
			public void setProperty(Object object, String key, Object value) {
				if (object instanceof SessionFile) {
					SessionFile f = (SessionFile) object;
					if(value==null){
						f.removeAttribute(key);
					}else if(value instanceof String){
						f.putAttribute(key, (String)value);
					}else{
						f.putAttribute(key, value.toString());
					}
					return;
				}
				super.setProperty(object, key, value);
			}
			
			@Override
			public Object invokeMethod(Object object, String methodName, Object[] args){
				if(object instanceof SessionFile){
					if( "write".equals(methodName) ) {
						if( args.length==1 && args[0] instanceof Closure) {
							this._write((SessionFile)object, (Closure)args[0]);
							return null;
						} else if(args.length==2) {
							if(args[0] instanceof String) {
								if(args[1] instanceof Closure) {
									this._write((SessionFile)object, (String)args[0], (Closure)args[1]);
									return null;
								} else if(args[1] instanceof CharSequence) {
									this._write((SessionFile)object, (String)args[0], (CharSequence)args[1]);
									return null;
								} else if(args[1] instanceof Writable) {
									this._write((SessionFile)object, (String)args[0], (CharSequence)args[1]);
									return null;
								}
							}
						}
					}
				}
				return super.invokeMethod(object, methodName, args);
			}
			
			private void _write(SessionFile f, String charset, Closure c){
				f.write(new OutputStreamCallback(){
					public void process(OutputStream out) throws IOException{
						Writer w = new OutputStreamWriter(out, charset);
						c.call(w);
						w.flush();
						w.close();
					}
				});
			}
			private void _write(SessionFile f, String charset, CharSequence c){
				f.write(new OutputStreamCallback(){
					public void process(OutputStream out) throws IOException{
						Writer w = new OutputStreamWriter(out, charset);
						w.append(c);
						w.flush();
						w.close();
					}
				});
			}
			private void _write(SessionFile f, String charset, Writable c){
				f.write(new OutputStreamCallback(){
					public void process(OutputStream out) throws IOException{
						Writer w = new OutputStreamWriter(out, charset);
						c.writeTo(w);
						w.flush();
						w.close();
					}
				});
			}
			private void _write(SessionFile f, Closure c) {
				if(c.getMaximumNumberOfParameters()==1){
					f.write(new OutputStreamCallback(){
						public void process(OutputStream out) throws IOException{
							c.call(out);
						}
					});
				}else{
					f.write(new StreamCallback(){
						public void process(InputStream in, OutputStream out) throws IOException{
							c.call(in,out);
						}
					});
				}
			}
			
		});
		return true;
	}
	
	private static boolean metaFlowFile(){
		GroovySystem.getMetaClassRegistry().setMetaClass(FlowFile.class,new DelegatingMetaClass(FlowFile.class) {
			@Override
			public Object getProperty(Object object, String key) {
				if (!fileProps.contains(key) && object instanceof SessionFile) {
					FlowFile f = (FlowFile) object;
					return f.getAttribute(key);
				}
				return super.getProperty(object, key);
			}
		});
		return true;
	}
	
	private static boolean metaRelationship(){
		GroovySystem.getMetaClassRegistry().setMetaClass(Relationship.class,new DelegatingMetaClass(Relationship.class) {
			@Override
			public Object invokeMethod(Object object, String methodName, Object[] args){
				if(object instanceof Relationship){
					if( "leftShift".equals(methodName)  && args.length==1 ){
						if(args[0] instanceof SessionFile){
							return this._leftShift((Relationship)object, (SessionFile)args[0]);
						}else if(args[0] instanceof Collection){
							return this._leftShift((Relationship)object, (Collection)args[0]);
						}
					}
				}
				return super.invokeMethod(object, methodName, args);
			}
			
			/** to support: REL_SUCCESS << sessionFile */
			private Relationship _leftShift(Relationship r, SessionFile f) {
				f.transfer(r);
				return r;
			}
			
			private Relationship _leftShift(Relationship r, Collection sfl) {
				if(sfl!=null && sfl.size()>0) {
					List<FlowFile> ffl = new ArrayList();
					ProcessSessionWrap session = null;
					//convert sessionfile collection to flowfile list
					for(Iterator i=sfl.iterator(); i.hasNext();){
						SessionFile f = (SessionFile)i.next();
						if(session==null)session=f.session();
						ffl.add(f.unwrap());
					}
					//assume all files has the same session
					session.transfer(ffl, r);
				}
				return r;
			}
			
		});
		return true;
	}
	private static boolean metaSession(){
		GroovySystem.getMetaClassRegistry().setMetaClass(ProcessSessionWrap.class,new DelegatingMetaClass(ProcessSessionWrap.class) {
			@Override
			public Object invokeMethod(Object object, String methodName, Object[] args){
				if(object instanceof ProcessSessionWrap){
					if( "get".equals(methodName)  && args.length==1 && args[0] instanceof Closure ){
						return this._get((ProcessSessionWrap)object, (Closure)args[0]);
					}
				}
				return super.invokeMethod(object, methodName, args);
			}
			/** makes closure to be used instead of FlowFileFilter. 
			 * Closure could return true to ACCEPT_AND_CONTINUE or false to REJECT_AND_CONTINUE
			 * or any of FlowFileFilterResult enums
			 */
			private List<FlowFile> _get(ProcessSessionWrap s, Closure filter) {
				return s.get(
					new FlowFileFilter(){
						public FlowFileFilterResult filter(FlowFile flowFile){
							Object res = filter.call(flowFile);
							if(res==null)return FlowFileFilterResult.REJECT_AND_TERMINATE;
							if(res instanceof Boolean){
								return ( ((Boolean)res).booleanValue() ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE );
							}
							if(res instanceof FlowFileFilterResult){
								return (FlowFileFilterResult)res;
							}
							return ( org.codehaus.groovy.runtime.DefaultGroovyMethods.asBoolean(res) ? FlowFileFilterResult.ACCEPT_AND_CONTINUE : FlowFileFilterResult.REJECT_AND_CONTINUE );
						}
					}
				);
			}
			
		});
		return true;
	}
}
