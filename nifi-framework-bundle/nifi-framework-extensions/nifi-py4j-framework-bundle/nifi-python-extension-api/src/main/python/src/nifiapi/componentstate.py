#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from enum import Enum
from nifiapi.__jvm__ import JvmHolder
from py4j.protocol import Py4JJavaError


CLUSTER_SCOPE = JvmHolder.jvm.org.apache.nifi.components.state.Scope.CLUSTER
LOCAL_SCOPE = JvmHolder.jvm.org.apache.nifi.components.state.Scope.LOCAL


def convert_pyscope_to_jscope(scope):
    """Converts Python Scope to Java Scope."""
    if scope == Scope.LOCAL:
        return LOCAL_SCOPE
    else:
        return CLUSTER_SCOPE

def convert_pydict_to_jmap(dictionary):
    """Converts a Python Dictionary to a Java Map"""
    java_map = JvmHolder.jvm.java.util.HashMap()
    for key, value in dictionary.items():
        java_map.put(key, value)
    return java_map

class Scope(Enum):
    CLUSTER = 1
    LOCAL = 2


class StateManager:
    """
    Python class wrapping the StateManager Java implementation.

    The constructor receives a Java object implementing the Java StateManager interface.
    The methods in Java StateManager have corresponding methods in this class.
    Each method in this class propagates the method call to the Java StateManager object.
    Each method may raise StateException in case the Java object fails to execute the particular operation.
    When an operation fails on the Java side, a Py4JJavaError is produced on the Python side by the Py4J bridge.
    The Py4JJavaError is caught in the Python StateManager and a new StateException instance is raised,
    indicating that the exception carries information regarding an error when accessing the state.
    """

    _java_state_manager = None

    def __init__(self, java_state_manager):
        """
        :param java_state_manager: Java StateManager object that performs all the StateManager-related operations.
        """
        self._java_state_manager = java_state_manager

    def setState(self, state, scope):
        """
        :param state: (dict) Python dictionary containing the state to be saved.
        :param scope: Scope.CLUSTER or Scope.LOCAL
        """
        try:
            self._java_state_manager.setState(convert_pydict_to_jmap(state), convert_pyscope_to_jscope(scope))
        except Py4JJavaError as exception:
            raise StateException("Set state failed") from exception

    def getState(self, scope):
        """
        :param scope: Scope.CLUSTER or Scope.LOCAL
        :return: Python StateMap object representing the state.
        """
        try:
            java_statemap = self._java_state_manager.getState(convert_pyscope_to_jscope(scope))
            return StateMap(java_statemap)
        except Py4JJavaError as exception:
            raise StateException('Get state failed') from exception

    def replace(self, oldValue, newValue, scope):
        """
        :param oldValue: (StateMap) Python StateMap object with the state to be replaced.
        :param newValue: (dict) Python dictionary containing the state to be saved.
        :param scope: Scope.CLUSTER or Scope.LOCAL
        """
        try:
            self._java_state_manager.replace(oldValue.get_java_statemap(), convert_pydict_to_jmap(newValue), convert_pyscope_to_jscope(scope))
        except Py4JJavaError as exception:
            raise StateException('Replace state failed') from exception

    def clear(self, scope):
        """
        :param scope: Scope.CLUSTER or Scope.LOCAL
        """
        try:
            self._java_state_manager.clear(convert_pyscope_to_jscope(scope))
        except Py4JJavaError as exception:
            raise StateException('Clear state failed') from exception


class StateMap:
    """
    Python class wrapping the StateMap Java implementation.

    The constructor receives a Java object implementing the Java StateMap interface.
    The methods in Java StateMap have corresponding methods in this class.
    Methods in this class propagate the method calls to the Java StateMap object.
    """

    _java_statemap = None
    _state_version = None

    def __init__(self, java_statemap):
        """
        :param java_statemap: Java StateMap object.
        """
        self._java_statemap = java_statemap
        if not java_statemap.getStateVersion().isEmpty():
            self._state_version = java_statemap.getStateVersion().get()

    def getStateVersion(self):
        """
        :return: (int) Integer value of the state version, or None if it is not set.
        """
        return self._state_version

    def get(self, key):
        """
        :param key: (str) String key indicating which state value should be returned.
        :return: (str) String state value.
        """
        return self._java_statemap.get(key)

    def toMap(self):
        """
        :return: (dict) Python dictionary representation of the state.
        """
        result = dict()

        java_iterator = self._java_statemap.toMap().entrySet().iterator()
        while java_iterator.hasNext():
            java_map_entry = java_iterator.next()
            result[java_map_entry.getKey()] = java_map_entry.getValue()

        return result

    def get_java_statemap(self):
        """
        :return: The Java StateMap object behind this wrapper class.
        """
        return self._java_statemap


class StateException(Exception):
    """
    Exception class for any exception produced by the operations in StateManager.
    """