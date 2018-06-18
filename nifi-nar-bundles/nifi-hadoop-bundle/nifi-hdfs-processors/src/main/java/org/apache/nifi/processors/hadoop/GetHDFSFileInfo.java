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
package org.apache.nifi.processors.hadoop;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.GetHDFSFileInfo.HDFSFileInfoRequest.Groupping;

@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"hadoop", "HDFS", "get", "list", "ingest", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files and directories from HDFS. "
        + "This processor creates a FlowFile(s) that represents the HDFS file/dir with relevant information. "
        + "Main purpose of this processor to provide functionality similar to HDFS Client, i.e. count, du, ls, test, etc. "
        + "Unlike ListHDFS, this processor is stateless, supports incoming connections and provides information on a dir level. "
        )
@WritesAttributes({
    @WritesAttribute(attribute="hdfs.objectName", description="The name of the file/dir found on HDFS."),
    @WritesAttribute(attribute="hdfs.path", description="The path is set to the absolute path of the object's parent directory on HDFS. "
            + "For example, if an object is a directory 'foo', under directory '/bar' then 'hdfs.objectName' will have value 'foo', and 'hdfs.path' will be '/bar'"),
    @WritesAttribute(attribute="hdfs.type", description="The type of an object. Possible values: directory, file, link"),
    @WritesAttribute(attribute="hdfs.owner", description="The user that owns the object in HDFS"),
    @WritesAttribute(attribute="hdfs.group", description="The group that owns the object in HDFS"),
    @WritesAttribute(attribute="hdfs.lastModified", description="The timestamp of when the object in HDFS was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
    @WritesAttribute(attribute="hdfs.length", description=""
            + "In case of files: The number of bytes in the file in HDFS.  "
            + "In case of dirs: Retuns storage space consumed by directory. "
            + ""),
    @WritesAttribute(attribute="hdfs.count.files", description="In case of type='directory' will represent total count of files under this dir. "
            + "Won't be populated to other types of HDFS objects. "),
    @WritesAttribute(attribute="hdfs.count.dirs", description="In case of type='directory' will represent total count of directories under this dir (including itself). "
            + "Won't be populated to other types of HDFS objects. "),
    @WritesAttribute(attribute="hdfs.replication", description="The number of HDFS replicas for the file"),
    @WritesAttribute(attribute="hdfs.permissions", description="The permissions for the object in HDFS. This is formatted as 3 characters for the owner, "
            + "3 for the group, and 3 for other users. For example rw-rw-r--"),
    @WritesAttribute(attribute="hdfs.status", description="The status contains comma separated list of file/dir paths, which couldn't be listed/accessed. "
            + "Status won't be set if no errors occured."),
    @WritesAttribute(attribute="hdfs.full.tree", description="When destination is 'attribute', will be populated with full tree of HDFS directory in JSON format."
            + "WARNING: In case when scan finds thousands or millions of objects, having huge values in attribute could impact flow file repo and GC/heap usage. "
            + "Use content destination for such cases")
})
@SeeAlso({ListHDFS.class, GetHDFS.class, FetchHDFS.class, PutHDFS.class})
public class GetHDFSFileInfo extends AbstractHadoopProcessor {
    public static final String APPLICATION_JSON = "application/json";
    public static final PropertyDescriptor FULL_PATH = new PropertyDescriptor.Builder()
            .displayName("Full path")
            .name("gethdfsfileinfo-full-path")
            .description("A directory to start listing from, or a file's full path.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
            .displayName("Recurse Subdirectories")
            .name("gethdfsfileinfo-recurse-subdirs")
            .description("Indicates whether to list files from subdirectories of the HDFS directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIR_FILTER = new PropertyDescriptor.Builder()
            .displayName("Directory Filter")
            .name("gethdfsfileinfo-dir-filter")
            .description("Regex. Only directories whose names match the given regular expression will be picked up. If not provided, any filter would be apply (performance considerations).")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .displayName("File Filter")
            .name("gethdfsfileinfo-file-filter")
            .description("Regex. Only files whose names match the given regular expression will be picked up. If not provided, any filter would be apply (performance considerations).")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor FILE_EXCLUDE_FILTER = new PropertyDescriptor.Builder()
            .displayName("Exclude Files")
            .name("gethdfsfileinfo-file-exclude-filter")
            .description("Regex. Files whose names match the given regular expression will not be picked up. If not provided, any filter won't be apply (performance considerations).")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
            .build();

    public static final PropertyDescriptor IGNORE_DOTTED_DIRS = new PropertyDescriptor.Builder()
            .displayName("Ignore Dotted Directories")
            .name("gethdfsfileinfo-ignore-dotted-dirs")
            .description("If true, directories whose names begin with a dot (\".\") will be ignored")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor IGNORE_DOTTED_FILES = new PropertyDescriptor.Builder()
            .displayName("Ignore Dotted Files")
            .name("gethdfsfileinfo-ignore-dotted-files")
            .description("If true, files whose names begin with a dot (\".\") will be ignored")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final AllowableValue GROUP_ALL = new AllowableValue("gethdfsfileinfo-group-all", "All",
            "Group all results into a single flowfile.");

    static final AllowableValue GROUP_PARENT_DIR = new AllowableValue("gethdfsfileinfo-group-parent-dir", "Parent Directory",
            "Group HDFS objects by their parent directories only. Processor will generate flowfile for each directory (if recursive). "
            + "If 'Recurse Subdirectories' property set to 'false', then will have the same effect as 'All'");

    static final AllowableValue GROUP_NONE = new AllowableValue("gethdfsfileinfo-group-none", "None",
            "Don't group results. Generate flowfile per each HDFS object.");

    public static final PropertyDescriptor GROUPING = new PropertyDescriptor.Builder()
            .displayName("Group Results")
            .name("gethdfsfileinfo-group")
            .description("Groups HDFS objects")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(GROUP_ALL, GROUP_PARENT_DIR, GROUP_NONE)
            .defaultValue(GROUP_ALL.getValue())
            .build();

    static final AllowableValue DESTINATION_ATTRIBUTES = new AllowableValue("gethdfsfileinfo-dest-attr", "Attributes",
            "Details of given HDFS object will be stored in attributes of flowfile. "
            + "WARNING: In case when scan finds thousands or millions of objects, having huge values in attribute could impact flow file repo and GC/heap usage. "
            + "Use content destination for such cases.");

    static final AllowableValue DESTINATION_CONTENT = new AllowableValue("gethdfsfileinfo-dest-content", "Content",
            "Details of given HDFS object will be stored in a content in JSON format");

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .displayName("Destination")
            .name("gethdfsfileinfo-destination")
            .description("Sets the destination for the resutls. When set to 'Content', attributes of flowfile won't be used for storing results. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(DESTINATION_ATTRIBUTES, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_CONTENT.getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All successfully generated FlowFiles are transferred to this relationship")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("If no objects are found, original FlowFile are transferred to this relationship")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original FlowFiles are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed attempts to access HDFS will be routed to this relationship")
            .build();

    private HDFSFileInfoRequest req;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        super.init(context);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(FULL_PATH);
        props.add(RECURSE_SUBDIRS);
        props.add(DIR_FILTER);
        props.add(FILE_FILTER);
        props.add(FILE_EXCLUDE_FILTER);
        props.add(IGNORE_DOTTED_DIRS);
        props.add(IGNORE_DOTTED_FILES);
        props.add(GROUPING);
        props.add(DESTINATION);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        // drop request details to rebuild it
        req = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile ff = null;
        if (context.hasIncomingConnection()) {
            ff = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (ff == null && context.hasNonLoopConnection()) {
                context.yield();
                return;
            }
        }
        boolean scheduledFF = false;
        if (ff == null) {
            ff = session.create();
            scheduledFF = true;
        }

        if (req == null) {
            //rebuild the request details based on
            req = buildRequestDetails(context, session, ff);
        }else {
            //avoid rebuilding req object's patterns in order to have better performance
            req = updateRequestDetails(context, session, ff);
        }

        try {
            final FileSystem hdfs = getFileSystem();
            UserGroupInformation ugi = getUserGroupInformation();
            HDFSObjectInfoDetails res = walkHDFSTree(context, session, ff, hdfs, ugi, req, null, false);
            if (res == null) {
                ff = session.putAttribute(ff, "hdfs.status", "Path not found: " + req.fullPath);
                session.transfer(ff, REL_NOT_FOUND);
                return;
            }
            if (!scheduledFF) {
                session.transfer(ff, REL_ORIGINAL);
            }else {
                session.remove(ff);
            }
        } catch (final IOException | IllegalArgumentException e) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {e});
            ff = session.putAttribute(ff, "hdfs.status", "Failed due to: " + e);
            session.transfer(ff, REL_FAILURE);
            return;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            getLogger().error("Interrupted while performing listing of HDFS", e);
            ff = session.putAttribute(ff, "hdfs.status", "Failed due to: " + e);
            session.transfer(ff, REL_FAILURE);
            return;
        } catch (final Exception e) {
            getLogger().error("Failed to perform listing of HDFS due to {}", new Object[] {e});
            ff = session.putAttribute(ff, "hdfs.status", "Failed due to: " + e);
            session.transfer(ff, REL_FAILURE);
            return;
        }
    }


    /*
     * Walks thru HDFS tree. This method will return null to the main if there is no provided path existing.
     */
    protected HDFSObjectInfoDetails walkHDFSTree(final ProcessContext context, final ProcessSession session, FlowFile origFF, final FileSystem hdfs,
            final UserGroupInformation ugi, final HDFSFileInfoRequest req, HDFSObjectInfoDetails parent, final boolean statsOnly) throws Exception{

        final HDFSObjectInfoDetails p = parent;

        if (!ugi.doAs((PrivilegedExceptionAction<Boolean>) () -> hdfs.exists(p != null ? p.getPath() : new Path(req.fullPath)))) {
                return null;
        }

        if (parent == null) {
            parent = new HDFSObjectInfoDetails(ugi.doAs((PrivilegedExceptionAction<FileStatus>) () -> hdfs.getFileStatus(new Path(req.fullPath))));
        }
        if (parent.isFile() && p == null) {
            //single file path requested and found, lets send to output:
            processHDFSObject(context, session, origFF, req, parent, true);
            return parent;
        }

        final Path path = parent.getPath();

        FileStatus[] listFSt = null;
        try {
            listFSt = ugi.doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfs.listStatus(path));
        }catch (IOException e) {
            parent.error = "Couldn't list directory: " + e;
            processHDFSObject(context, session, origFF, req, parent, p == null);
            return parent; //File not found exception, or access denied - don't interrupt, just don't list
        }
        if (listFSt != null) {
            for (FileStatus f : listFSt) {
                HDFSObjectInfoDetails o = new HDFSObjectInfoDetails(f);
                HDFSObjectInfoDetails vo = validateMatchingPatterns(o, req);
                if (o.isDirectory() && !o.isSymlink() && req.isRecursive) {
                    o = walkHDFSTree(context, session, origFF, hdfs, ugi, req, o, vo == null || statsOnly);
                    parent.countDirs += o.countDirs;
                    parent.totalLen += o.totalLen;
                    parent.countFiles += o.countFiles;
                }else if (o.isDirectory() && o.isSymlink()) {
                    parent.countDirs += 1;
                }else if (o.isFile() && !o.isSymlink()) {
                    parent.countFiles += 1;
                    parent.totalLen += o.getLen();
                }else if (o.isFile() && o.isSymlink()) {
                    parent.countFiles += 1; // do not add length of the symlink, as it doesn't consume space under THIS directory, but count files, as it is still an object.
                }

                // Decide what to do with child: if requested FF per object or per dir - just emit new FF with info in 'o' object
                if (vo != null && !statsOnly) {
                    parent.addChild(vo);
                    if (p != null && req.isRecursive
                            && vo.isFile() && !vo.isSymlink()) {
                        processHDFSObject(context, session, origFF, req, vo, false);
                    }
                }
            }
            if (!statsOnly) {
                processHDFSObject(context, session, origFF, req, parent, p==null);
            }
            if (req.groupping != Groupping.ALL) {
                parent.setChildren(null); //we need children in full tree only when single output requested.
            }
        }

        return parent;
    }

    protected HDFSObjectInfoDetails validateMatchingPatterns(final HDFSObjectInfoDetails o, HDFSFileInfoRequest req) {
        if (o == null || o.getPath() == null) {
            return null;
        }

        if (o.isFile()) {
            if (req.isIgnoreDotFiles && o.getPath().getName().startsWith(".")) {
                return null;
            }else if (req.fileExcludeFilter != null && req.fileExcludeFilter.matcher(o.getPath().getName()).matches()) {
                return null;
            }else if (req.fileFilter == null) {
                return o;
            }else if (req.fileFilter != null && req.fileFilter.matcher(o.getPath().getName()).matches()) {
                return o;
            }else {
                return null;
            }
        }
        if (o.isDirectory()) {
            if (req.isIgnoreDotDirs && o.getPath().getName().startsWith(".")) {
                return null;
            }else if (req.dirFilter == null) {
                return o;
            }else if (req.dirFilter != null && req.dirFilter.matcher(o.getPath().getName()).matches()) {
                return o;
            }else {
                return null;
            }
        }
        return null;
    }

    /*
     * Checks whether HDFS object should be sent to output.
     * If it should be sent, new flowfile will be created, its content and attributes will be populated according to other request params.
     */
    protected HDFSObjectInfoDetails processHDFSObject(final ProcessContext context, final ProcessSession session,
                    FlowFile origFF, final HDFSFileInfoRequest req, final HDFSObjectInfoDetails o, final boolean isRoot) {
        if (o.isFile() && req.groupping != Groupping.NONE) {
            return null; //there is grouping by either root directory or every directory, no need to print separate files.
        }
        if (o.isDirectory() && o.isSymlink() && req.groupping != Groupping.NONE) {
            return null; //ignore symlink dirs an
        }
        if (o.isDirectory() && req.groupping == Groupping.ALL && !isRoot) {
            return null;
        }
        FlowFile ff = session.create(origFF);

        //if destination type is content - always add mime type
        if (req.isDestContent) {
            ff = session.putAttribute(ff, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
        }

        //won't combine conditions for similar actions for better readability and maintenance.
        if (o.isFile() && isRoot &&  req.isDestContent) {
            ff = session.write(ff, (out) ->  out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isFile() && isRoot &&  !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            // ------------------------------
        }else if (o.isFile() && req.isDestContent) {
            ff = session.write(ff, (out) -> out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isFile() && !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            // ------------------------------
        }else if (o.isDirectory() && o.isSymlink() && req.isDestContent) {
            ff = session.write(ff, (out) -> out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isDirectory() && o.isSymlink() && !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.NONE && req.isDestContent) {
            o.setChildren(null);
            ff = session.write(ff, (out) -> out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.NONE && !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.DIR && req.isDestContent) {
            ff = session.write(ff, (out) -> out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.DIR && !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            ff = session.putAttribute(ff, "hdfs.full.tree", o.toJsonString());
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.ALL && req.isDestContent) {
            ff = session.write(ff, (out) -> out.write(o.toJsonString().getBytes()));
            // ------------------------------
        }else if (o.isDirectory() && req.groupping == Groupping.ALL && !req.isDestContent) {
            ff = session.putAllAttributes(ff, o.toAttributesMap());
            ff = session.putAttribute(ff, "hdfs.full.tree", o.toJsonString());
        }else {
            getLogger().error("Illegal State!");
            session.remove(ff);
            return null;
        }

        session.transfer(ff, REL_SUCCESS);
        return o;
    }

    /*
     * Returns permissions in readable format like rwxr-xr-x (755)
     */
    protected String getPerms(final FsPermission permission) {

        final StringBuilder sb = new StringBuilder();
        for (FsAction action : new FsAction[]{permission.getUserAction(), permission.getGroupAction(), permission.getOtherAction()}) {
            if (action.implies(FsAction.READ)) {
                sb.append("r");
            } else {
                sb.append("-");
            }

            if (action.implies(FsAction.WRITE)) {
                sb.append("w");
            } else {
                sb.append("-");
            }

            if (action.implies(FsAction.EXECUTE)) {
                sb.append("x");
            } else {
                sb.append("-");
            }
        }

        return sb.toString();
    }

    /*
     * Creates internal request object and initialize the fields that won't be changed every call (onTrigger).
     * Dynamic fields will be updated per each call separately.
     */
    protected HDFSFileInfoRequest buildRequestDetails(ProcessContext context, ProcessSession session, FlowFile ff) {
        HDFSFileInfoRequest req = new HDFSFileInfoRequest();
        req.fullPath = context.getProperty(FULL_PATH).evaluateAttributeExpressions(ff).getValue();
        req.isRecursive = context.getProperty(RECURSE_SUBDIRS).asBoolean();

        PropertyValue pv = null;
        String v = null;

        if (context.getProperty(DIR_FILTER).isSet() && (pv=context.getProperty(DIR_FILTER).evaluateAttributeExpressions(ff))!=null) {
            v = pv.getValue();
            req.dirFilter = v == null ? null : Pattern.compile(v);
        }

        if (context.getProperty(FILE_FILTER).isSet() && (pv=context.getProperty(FILE_FILTER).evaluateAttributeExpressions(ff))!=null) {
            v = pv.getValue();
            req.fileFilter = v == null ? null : Pattern.compile(v);
        }

        if (context.getProperty(FILE_EXCLUDE_FILTER).isSet() && (pv=context.getProperty(FILE_EXCLUDE_FILTER).evaluateAttributeExpressions(ff))!=null) {
            v = pv.getValue();
            req.fileExcludeFilter = v == null ? null : Pattern.compile(v);
        }

        req.isIgnoreDotFiles = context.getProperty(IGNORE_DOTTED_FILES).asBoolean();
        req.isIgnoreDotDirs = context.getProperty(IGNORE_DOTTED_DIRS).asBoolean();

        req.groupping = HDFSFileInfoRequest.Groupping.getEnum(context.getProperty(GROUPING).getValue());

        v = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.getValue().equals(v)) {
            req.isDestContent = true;
        }else {
            req.isDestContent = false;
        }

        return req;
    }

    /*
     * Creates internal request object if not created previously, and updates it with dynamic property every time onTrigger is called.
     * Avoids creating regex Patter objects unless their actual value are changed due to evaluation of EL
     */
    protected HDFSFileInfoRequest updateRequestDetails(ProcessContext context, ProcessSession session, FlowFile ff) {

        if (req == null) {
            return buildRequestDetails(context, session, ff);
        }
        req.fullPath = context.getProperty(FULL_PATH).evaluateAttributeExpressions(ff).getValue();

        String currValue = null;
        String oldValue = null;

        currValue = context.getProperty(DIR_FILTER).evaluateAttributeExpressions(ff).getValue();
        oldValue = req.dirFilter == null ? null : req.dirFilter.toString();
        if (StringUtils.compare(currValue, oldValue) != 0) {
            req.dirFilter = currValue == null ? null : Pattern.compile(currValue);
        }


        currValue = context.getProperty(FILE_FILTER).evaluateAttributeExpressions(ff).getValue();
        oldValue = req.fileFilter == null ? null : req.fileFilter.toString();
        if (StringUtils.compare(currValue, oldValue) != 0) {
            req.fileFilter = currValue == null ? null : Pattern.compile(currValue);
        }


        currValue = context.getProperty(FILE_EXCLUDE_FILTER).evaluateAttributeExpressions(ff).getValue();
        oldValue = req.fileExcludeFilter == null ? null : req.fileExcludeFilter.toString();
        if (StringUtils.compare(currValue, oldValue) != 0) {
            req.fileExcludeFilter = currValue == null ? null : Pattern.compile(currValue);
        }

        return req;
    }

    /*
     * Keeps all request details in single object.
     */
    static class HDFSFileInfoRequest{
        enum Groupping {
            ALL(GROUP_ALL.getValue()),
            DIR(GROUP_PARENT_DIR.getValue()),
            NONE(GROUP_NONE.getValue());

            private String val;

            Groupping(String val){
                this.val = val;
            }

            public String toString() {
                return this.val;
            }

            public static Groupping getEnum(String value) {
                for (Groupping v : values()) {
                    if (v.val.equals(value)) {
                        return v;
                    }
                }
                return null;
            }
        }

        String fullPath;
        boolean isRecursive;
        Pattern dirFilter;
        Pattern fileFilter;
        Pattern fileExcludeFilter;
        boolean isIgnoreDotFiles;
        boolean isIgnoreDotDirs;
        boolean isDestContent;
        Groupping groupping;
    }

    /*
     * Keeps details of HDFS objects.
     * This class is based on FileStatus and adds additional feature/properties for count, total size of directories, and subtrees/hierarchy of recursive listings.
     */
    class HDFSObjectInfoDetails extends FileStatus{

        private long countFiles;
        private long countDirs = 1;
        private long totalLen;
        private Collection<HDFSObjectInfoDetails> children = new LinkedList<>();
        private String error;

        HDFSObjectInfoDetails(FileStatus fs) throws IOException{
            super(fs);
        }

        public long getCountFiles() {
            return countFiles;
        }

        public void setCountFiles(long countFiles) {
            this.countFiles = countFiles;
        }

        public long getCountDirs() {
            return countDirs;
        }

        public void setCountDirs(long countDirs) {
            this.countDirs = countDirs;
        }

        public long getTotalLen() {
            return totalLen;
        }

        public void setTotalLen(long totalLen) {
            this.totalLen = totalLen;
        }

        public Collection<HDFSObjectInfoDetails> getChildren() {
            return children;
        }

        public String getError() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public void setChildren(Collection<HDFSObjectInfoDetails> children) {
            this.children = children;
        }

        public void addChild(HDFSObjectInfoDetails child) {
            this.children.add(child);
        }

        public void updateTotals(boolean deepUpdate) {
            if (deepUpdate) {
                this.countDirs = 1;
                this.countFiles = 0;
                this.totalLen = 0;
            }

            for(HDFSObjectInfoDetails c : children) {
                if (c.isSymlink()) {
                    continue; //do not count symlinks. they either will be counted under their actual directories, or won't be count if actual location is not under provided root for scan.
                }else if (c.isDirectory()) {
                    if (deepUpdate) {
                        c.updateTotals(deepUpdate);
                    }
                    this.totalLen += c.totalLen;
                    this.countDirs += c.countDirs;
                    this.countFiles += c.countFiles;
                }else if (c.isFile()) {
                    this.totalLen += c.getLen();
                    this.countFiles++;
                }
            }

        }

        /*
         * Since, by definition, FF will keep only attributes for parent/single object, we don't need to recurse the children
         */
        public Map<String, String> toAttributesMap(){
            Map<String, String> map = new HashMap<>();

            map.put("hdfs.objectName", this.getPath().getName());
            map.put("hdfs.path", Path.getPathWithoutSchemeAndAuthority(this.getPath().getParent()).toString());
            map.put("hdfs.type", this.isSymlink() ? "link" : (this.isDirectory() ? "directory" : "file"));
            map.put("hdfs.owner", this.getOwner());
            map.put("hdfs.group", this.getGroup());
            map.put("hdfs.lastModified", Long.toString(this.getModificationTime()));
            map.put("hdfs.length", Long.toString(this.isDirectory() ? this.totalLen : this.getLen()));
            map.put("hdfs.replication", Long.toString(this.getReplication()));
            if (this.isDirectory()) {
                map.put("hdfs.count.files", Long.toString(this.getCountFiles()));
                map.put("hdfs.count.dirs", Long.toString(this.getCountDirs()));
            }
            map.put("hdfs.permissions", getPerms(this.getPermission()));
            if (this.error != null) {
                map.put("hdfs.status", "Error: " + this.error);
            }

            return map;
        }

        /*
         * The decision to use custom serialization (vs jackson/velocity/gson/etc) is behind the performance.
         * This object is pretty simple, with limited number of members of simple types.
         */
        public String toJsonString() {
            StringBuilder sb = new StringBuilder();
            return toJsonString(sb).toString();
        }

        private StringBuilder toJsonString(StringBuilder sb) {
            sb.append("{");

            appendProperty(sb, "objectName", this.getPath().getName()).append(",");
            appendProperty(sb, "path", Path.getPathWithoutSchemeAndAuthority(this.getPath().getParent()).toString()).append(",");
            appendProperty(sb, "type", this.isSymlink() ? "link" : (this.isDirectory() ? "directory" : "file")).append(",");
            appendProperty(sb, "owner", this.getOwner()).append(",");
            appendProperty(sb, "group", this.getGroup()).append(",");
            appendProperty(sb, "lastModified", Long.toString(this.getModificationTime())).append(",");
            appendProperty(sb, "length", Long.toString(this.isDirectory() ? this.totalLen : this.getLen())).append(",");
            appendProperty(sb, "replication", Long.toString(this.getReplication())).append(",");
            if (this.isDirectory()) {
                appendProperty(sb, "countFiles", Long.toString(this.getCountFiles())).append(",");
                appendProperty(sb, "countDirs", Long.toString(this.getCountDirs())).append(",");
            }
            appendProperty(sb, "permissions", getPerms(this.getPermission()));

            if (this.error != null) {
                sb.append(",");
                appendProperty(sb, "status", this.error);
            }

            if (this.getChildren() != null && this.getChildren().size() > 0) {
                sb.append(",\"content\":[");
                for (HDFSObjectInfoDetails c : this.getChildren()) {
                    c.toJsonString(sb).append(",");
                }
                sb.deleteCharAt(sb.length()-1).append("]");
            }
            sb.append("}");
            return sb;
        }

        private StringBuilder appendProperty(StringBuilder sb, String name, String value) {
            return sb.append("\"").append(name).append("\":\"").append(value == null? "": value).append("\"");
        }
    }
}
