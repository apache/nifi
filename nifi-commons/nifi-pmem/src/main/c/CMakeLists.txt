# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.5)
project(nifi-pmem)

# Target: Shared library.
add_library(PmemMappedFile SHARED PmemMappedFile.c)

# JNI headers.
find_package(JNI)
if(NOT JNI_FOUND)
	message(FATAL_ERROR "JNI not found.")
endif()
target_include_directories(PmemMappedFile PRIVATE ${JAVA_INCLUDE_PATH})
target_include_directories(PmemMappedFile PRIVATE ${JAVA_INCLUDE_PATH2})

# OS-specific configurations.
if(UNIX)
	# Options for preprocessor.
	#
	# Note that "-DNDEBUG" may be given, depending on -DCMAKE_BUILD_TYPE=
	# *FooBar*.
	#
	# Multi-thread awareness. "200112L" is for XSI-compliant strerror_r.
	target_compile_definitions(PmemMappedFile PRIVATE _POSIX_C_SOURCE=200112L)
	#
	# 64-bit file system interface.
	target_compile_definitions(PmemMappedFile PRIVATE _FILE_OFFSET_BITS=64)

	# Options for compiler.
	#
	# Note that "-fPIC" will be given by add_library.
	#
	# Also note that debug (such as -g) and optimization (such as -O2) options
	# may be given, depending on -DCMAKE_BUILD_TYPE=*FooBar*.
	#
	target_compile_options(PmemMappedFile PRIVATE -Wall -Wextra -Werror)
	#
	# For native functions with unused parameters.
	target_compile_options(PmemMappedFile PRIVATE -Wno-unused-parameter)

	# Options for linker.
	#
	# Note that "-shared" will be given by add_library.
	#
	target_link_libraries(PmemMappedFile PRIVATE pmem)

endif()
