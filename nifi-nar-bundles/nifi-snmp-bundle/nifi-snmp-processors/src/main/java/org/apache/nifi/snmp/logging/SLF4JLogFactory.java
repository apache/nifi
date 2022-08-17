/*_############################################################################
  _##
  _##  SNMP4J - JavaLogFactory.java
  _##
  _##  Copyright (C) 2003-2020  Frank Fock (SNMP4J.org)
  _##
  _##  Licensed under the Apache License, Version 2.0 (the "License");
  _##  you may not use this file except in compliance with the License.
  _##  You may obtain a copy of the License at
  _##
  _##      http://www.apache.org/licenses/LICENSE-2.0
  _##
  _##  Unless required by applicable law or agreed to in writing, software
  _##  distributed under the License is distributed on an "AS IS" BASIS,
  _##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  _##  See the License for the specific language governing permissions and
  _##  limitations under the License.
  _##
  _##########################################################################*/
package org.apache.nifi.snmp.logging;

import org.slf4j.LoggerFactory;
import org.snmp4j.log.LogAdapter;
import org.snmp4j.log.LogFactory;

import java.util.Iterator;

public class SLF4JLogFactory extends LogFactory {

    @Override
    protected LogAdapter createLogger(final Class loggerClass) {
        return new SLF4JLogAdapter(LoggerFactory.getLogger(loggerClass.getName()));
    }

    @Override
    protected LogAdapter createLogger(final String className) {
        return new SLF4JLogAdapter(LoggerFactory.getLogger(className));
    }

    @Override
    public LogAdapter getRootLogger() {
        return new SLF4JLogAdapter(LoggerFactory.getLogger(""));
    }

    @Override
    public Iterator loggers() {
        throw new UnsupportedOperationException("Iterators are currently not supported!");
    }
}
