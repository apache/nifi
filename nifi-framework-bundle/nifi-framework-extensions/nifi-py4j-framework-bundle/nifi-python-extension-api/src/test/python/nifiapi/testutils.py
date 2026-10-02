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


class AutoAttr:
    """Stands in for a py4j JVM/gateway view. Any attribute access or call returns another
    AutoAttr, so module-level lookups like JvmHolder.jvm.org.apache.nifi... succeed without
    needing to model the full Java package tree."""

    def __getattr__(self, name):
        return AutoAttr()

    def __call__(self, *args, **kwargs):
        return AutoAttr()


def set_up_env():
    from nifiapi.__jvm__ import JvmHolder
    JvmHolder.jvm = AutoAttr()
    JvmHolder.gateway = AutoAttr()
