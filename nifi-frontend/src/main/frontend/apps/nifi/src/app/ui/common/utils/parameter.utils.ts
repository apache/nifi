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

/**
 * Extracts the first parameter name from a NiFi parameter reference expression.
 *
 * Supported forms (anywhere within the value):
 *   #{name}
 *   #{'name'}
 *   #{"name"}
 *
 * Returns the name from the first `#{…}` reference found in the value, or
 * `undefined` when no reference is present or the reference contains an empty
 * name.
 *
 * When the value embeds a reference among other content (`prefix #{param} suffix`)
 * or contains multiple references (`#{p1} and #{p2}`), the name from the first
 * reference is returned. This is deterministic, matches reading order, and avoids
 * a no-op fallback. A future enhancement could surface a sub-menu for all
 * referenced parameters (analogous to the `PropertyValueTip` tooltip), allowing
 * the user to choose which one to navigate to.
 */
export function extractParameterName(value: string): string | undefined {
    const match = /#{(['"]?)([^}]+)\1}/.exec(value);
    return match?.[2];
}
