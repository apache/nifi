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


def evaluate(record, schema, record_path):
    """Evaluate a RecordPath-like expression against the provided record.

    This lightweight implementation supports simple '/' separated paths with
    optional zero-based list indexes (for example '/parent/child[0]'). It
    returns wrappers that allow callers to read or update the targeted value.
    """
    if record is None or record_path is None:
        return RecordPathResult(record, None, [], dirty=False)

    wrappers = _evaluate_simple_path(record, record_path)
    return RecordPathResult(record, None, wrappers, dirty=False)


def _evaluate_simple_path(record, record_path):
    tokens = [segment for segment in record_path.split('/') if segment]
    if not tokens:
        return []

    current = record
    parent = None
    key = None

    for token in tokens:
        if '[' in token and token.endswith(']'):
            base, index_text = token.split('[', 1)
            index_text = index_text[:-1]
        else:
            base, index_text = token, None

        if not isinstance(current, dict) or base not in current:
            return []

        parent = current
        key = base
        current = current[base]

        if index_text is not None:
            if not isinstance(current, list):
                return []
            try:
                index = int(index_text)
            except ValueError:
                return []
            if index < 0 or index >= len(current):
                return []
            parent = current
            key = index
            current = current[index]

    if parent is None:
        return []

    return [_SimpleFieldValueWrapper(parent, key)]


class RecordPathResult:
    def __init__(self, python_record, java_record, field_wrappers, dirty=True):
        self._python_record = python_record
        self._java_record = java_record
        self._field_wrappers = field_wrappers
        self._dirty = False

        for wrapper in self._field_wrappers:
            wrapper._attach(self)

    def __iter__(self):
        return iter(self._field_wrappers)

    def is_empty(self):
        return len(self._field_wrappers) == 0

    def fields(self):
        return list(self._field_wrappers)

    def apply(self):
        return self._python_record

    def _mark_dirty(self):
        self._dirty = True


class _SimpleFieldValueWrapper:
    def __init__(self, parent, key):
        self._parent = parent
        self._key = key
        self._parent_result = None

    def _attach(self, parent_result):
        self._parent_result = parent_result

    def get_value(self):
        try:
            return self._parent[self._key]
        except Exception:
            return None

    def set_value(self, new_value):
        self.update_value(new_value)

    def update_value(self, new_value):
        try:
            self._parent[self._key] = new_value
            if self._parent_result is not None:
                self._parent_result._mark_dirty()
        except Exception:
            pass

    def get_field(self):
        return None

    def get_parent(self):
        return None
