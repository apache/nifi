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
package org.apache.nifi.rwfile;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;

/**
 * @author every
 */
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "fetch"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile. Once this is done, the file is optionally moved " +
        "elsewhere or deleted to help keep the file system organized.")
public class StandardReadFileService extends AbstractControllerService implements ReadFileService {


    public static Map<String, Map> catchfile = null;
    private static List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        properties = Collections.unmodifiableList(props);
        catchfile = new ConcurrentHashMap();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

    }

    @OnDisabled
    public void shutdown() {
        catchfile.clear();
    }

    @Override
    public void execute() throws ProcessException {

    }

    @Override
    public Map readFile(String filepath,int ignore_header,String charSet,int read_rows) {
        final File file = new File(filepath);
        boolean over = true;
        long i = 0L;
        long r = 0L;
        int fragment_readline = read_rows;
        Map rtn = new HashMap();
        try{
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader isr=new InputStreamReader(fis, charSet);
            BufferedReader br = new BufferedReader(isr);
            StringBuffer rows = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                if (ignore_header > 0) {
                    ignore_header--;
                    continue;
                }
                r++;
                rows.append(line).append("\n");
                if (read_rows!=-1){
                    if(--read_rows==0){
                        over = false;
                        break;
                    }
                }
            }
            rtn.put("data",rows.toString());
            rtn.put("fragment_index",++i);
            rtn.put("read_count",r);
            rtn.put("over",over);
            if(!over) {
                Map file_info = new HashMap();
                file_info.put("file",file);
                file_info.put("br",br);
                file_info.put("isr",isr);
                file_info.put("fis",fis);
                file_info.put("index",i);
                file_info.put("rows",r);
                file_info.put("read_rows",fragment_readline);
                catchfile.put(filepath,file_info);
            }
        } catch (IOException ioe) {
            getLogger().error("Could not fetch file {} from file system for {} ", new Object[]{file, ioe.toString()}, ioe);
        } catch (Throwable ioe) {
            getLogger().error("msg:{}",new Object[]{ioe.getMessage()},ioe);
        }
        return rtn;
    }

    @Override
    public Map getFragment(String filepath){
        Map rtn = new HashMap();

        getLogger().debug("catchfile:"+catchfile);
        try{
            Map file_info = catchfile.get(filepath);
            if(file_info == null){
                return null;
            }
            BufferedReader br = (BufferedReader) file_info.get("br");
            long i = (long) file_info.get("index");
            long r = (long) file_info.get("rows");
            int read_rows = (int) file_info.get("read_rows");
            boolean over = true;
            StringBuffer rows = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                r++;
                rows.append(line).append("\n");
                if (read_rows!=-1){
                    if(--read_rows==0){
                        over = false;
                        break;
                    }
                }
            }
            rtn.put("data",rows.toString());
            rtn.put("fragment_index",++i);
            rtn.put("rows",r);
            rtn.put("over",over);
            if(!over) {
                file_info.put("index",++i);
                file_info.put("rows",r);
            }else{
                catchfile.remove(filepath);

                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        getLogger().error(e.getMessage(), e);
                    }
                }
                InputStreamReader isr = (InputStreamReader) file_info.get("isr");
                FileInputStream fis = (FileInputStream) file_info.get("fis");
                if(isr!=null){
                    try {
                        isr.close();
                    } catch (IOException e) {
                        getLogger().error(e.getMessage(), e);
                    }
                }
                if(fis!=null){
                    try {
                        fis.close();
                    } catch (IOException e) {
                        getLogger().error(e.getMessage(), e);
                    }
                }
            }
        } catch (IOException ioe) {
            getLogger().error("Could not fetch file from file system for {} ", new Object[]{ ioe.toString()}, ioe);
        } catch (Throwable ioe) {
            getLogger().error("msg:{}",new Object[]{ioe.getMessage()},ioe);
        }
        return rtn;
    }

    @Override
    public Map closeFile(String filepath) {
        Map rtn = new HashMap();
        Map file_info = catchfile.remove(filepath);
        BufferedReader br = (BufferedReader) file_info.remove("br");
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                getLogger().error(e.getMessage(), e);
            }
        }
        InputStreamReader isr = (InputStreamReader) file_info.remove("isr");
        FileInputStream fis = (FileInputStream) file_info.remove("fis");
        if(isr!=null){
            try {
                isr.close();
            } catch (IOException e) {
                getLogger().error(e.getMessage(), e);
            }
        }
        if(fis!=null){
            try {
                fis.close();
            } catch (IOException e) {
                getLogger().error(e.getMessage(), e);
            }
        }
        file_info = null;
        return rtn;
    }

}
