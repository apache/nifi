<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# JASN1Reader

## Summary

This service creates record readers for ASN.1 input.

ASN.1 schema files (with full path) can be defined via the _ASN.1 Files_ property as a comma separated list. The
controller service preprocesses these files and generates sources that it uses for parsing data later.  
**Note that this preprocessing may take a while, especially when the schema files are large. The service remains in
the _Enabling_ state until this preprocessing is finished. Processors using the service are ready to be started at this
point but probably won't work properly until the service is fully _Enabled_.**  
**Also note that the preprocessing phase can fail if there are problems with the ASN.1 schema files. The bulletin - as
per usual - will show error messages related to the failure but interpreting those messages may not be straightforward.
For help troubleshooting such messages please refer to the _Troubleshooting_ section below.**

The root model type can be defined via the _Root Model Name_ property. It's format should be "MODULE-NAME.ModelType". 
"MODULE-NAME" is the name of the ASN module defined at the beginnig of the ASN schema file. "ModelType" is an ASN type
defined in the ASN files that is not referenced by any other type. The reader created by this service expects ASN
records of this root model type.

More than one root model types can be defined in the ASN schema files but one service instance can only work with one
such type at a time. Multiple different ASN data types can be processed by creating multiple instances of this service.

The ASN schema files are ultimately compiled into Java classes in a temporary directory when the service is enabled. 
(The directory is deleted when the service is disabled. Of course the ASN schema files remain.) The service actually
needs the fully qualified name of the class compiled from the root model type. It usually guesses the name of this class
correctly from _Root Model Name_.  
However there may be situations where this is not the case. Should this happen, one can take use of the fact that NiFi
logs the temporary directory where the compiled Java classes can be found. Once the proper class of the root model type
is identified in that directory (should be easily done by looking for it by its name) it can be provided directly via
the _Root Model Class Name_ property. (Note however that the service should be left _Enabled_ while doing the search as
it deletes the temporary directory when it is disabled. To be able to set the property the service needs to be disabled
in the end - and let it remove the directory, however this shouldn't be an issue as the name of the root model class
will be the same in the new temporary directory.)

### Troubleshooting

The preprocessing is done in two phases:

1. The first phase reads the ASN.1 schema files and parses them. Formatting errors are usually reported during this
   phase. Here are some possible error messages and the potential root causes of the issues:
    * _line NNN:MMM: unexpected token: someFieldName_ - On the NNNth line, starting at the MMMth position,
      _someFieldName_ is encountered which was unexpected. Usually this means _someFieldName_ itself is fine but the
      previous field declaration doesn't have a comma ',' at the end.
    * _line NNN:MMM: unexpected token: \[_ - On the NNNth line, starting at the MMMth position, the opening square
      bracket '\[' is encountered which was unexpected. Usually this is the index part of the field declaration and for
      some reason the field declaration is invalid. This can typically occur if the field name is invalid, e.g. starts
      with an uppercase letter. (Field names must start with a lowercase letter.)
2. The second phase compiles the ASN.1 schema files into Java classes. Even if the ASN.1 files meet the formal
   requirements, due to the nature of the created Java files there are some extra limitations:
    * On certain systems type names are treated as case-insensitive. Because of this, two types whose names only differ
      in the cases of their letters may cause errors. For example if the ASN.1 schema files define both '
      SameNameWithDifferentCase' and 'SAMENAMEWithDifferentCase', the following error may be reported:

      _class SAMENAMEWithDifferentCase is public, should be declared in a file named SAMENAMEWithDifferentCase.java_
    * Certain keywords cannot be used as field names. Known reserved keywords and the corresponding reported error
      messages are:

    * length

   _incompatible types: com.beanit.asn1bean.ber.types.BerInteger cannot be converted to
   com.beanit.asn1bean.ber.BerLength_  
   _incompatible types: boolean cannot be converted to java.io.OutputStream_  
   _Some messages have been simplified; recompile with -Xdiags:verbose to get full output_

### Additional Preprocessing

NiFi doesn't support every feature that the ASN standard allows. To alleviate problems when encountering ASN files with
unsupported features, NiFi can do additional preprocessing steps that creates modified versions of the provided ASN
files, removing unsupported features in a way that makes them less strict but otherwise should still be compatible with
incoming data. This feature can be switched on via the 'Do Additional Preprocessing' property. The original files will
remain intact and new ones will be created with the same names in a directory set in the 'Additional Preprocessing
Output Directory' property. Please note that this is a best-effort attempt. It is also strongly recommended to compare
the resulting ASN files to the originals and make sure they are still appropriate.

The following modification are applied:

1. Constraints - Advanced constraints are not recognized as valid ASN elements by NiFi. This step will try to remove
   _all_ types of constraints.  
   E.g.

   field \[3\] INTEGER(SIZE(1..8,...,10|12|20)) OPTIONAL

   will be changed to

   field \[3\] INTEGER OPTIONAL

2. Version brackets - NiFi will try to remove all version brackets and leave all defined fields as OPTIONAL.  
   E.g.

   MyType ::= SEQUENCE {
   integerField1 INTEGER,
   integerField2 INTEGER,
   ..., -- comment1
   \[\[ -- comment2
   integerField3 INTEGER,
   integerField4 INTEGER,
   integerField5 INTEGER \]\]
   }

    will be changed to
    
    MyType ::= SEQUENCE {
    	integerField1		INTEGER,
    	integerField2		INTEGER,
    	...,	-- comment1
     -- comment2
    	integerField3		INTEGER OPTIONAL,
    	integerField4		INTEGER OPTIONAL,
    	integerField5		INTEGER OPTIONAL
    }

3. "Hugging" comments - This is not really an ASN feature but a potential error. The double dash comment indicator "--"
   should be separated from ASN elements.  
   E.g.

   field \[0\] INTEGER(1..8)--comment

   will be changed to

   field \[0\] INTEGER(1..8) --comment