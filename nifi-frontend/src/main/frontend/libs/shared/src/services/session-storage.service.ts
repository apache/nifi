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

import { Injectable } from '@angular/core';

interface SessionStorageEntry<T> {
    item: T;
}

@Injectable({
    providedIn: 'root'
})
export class SessionStorageService {
    /**
     * Gets an entry for the key. The entry expiration is not checked.
     *
     * @param {string} key
     */
    private getEntry<T>(key: string): null | SessionStorageEntry<T> {
        try {
            // parse the entry
            const item = sessionStorage.getItem(key);
            if (!item) {
                return null;
            }

            const entry = JSON.parse(item);

            // ensure the entry is present
            if (entry) {
                return entry;
            } else {
                return null;
            }
        } catch (e) {
            return null;
        }
    }

    /**
     * Stores the specified item.
     *
     * @param {string} key
     * @param {object} item
     */
    public setItem<T>(key: string, item: T): void {
        // create the entry
        const entry: SessionStorageEntry<T> = {
            item
        };

        // store the item
        sessionStorage.setItem(key, JSON.stringify(entry));
    }

    /**
     * Returns whether there is an entry for this key. This will not check the expiration. If
     * the entry is expired, it will return null on a subsequent getItem invocation.
     *
     * @param {string} key
     * @returns {boolean}
     */
    public hasItem(key: string): boolean {
        return this.getEntry(key) !== null;
    }

    /**
     * Gets the item with the specified key. If an item with this key does
     * not exist, null is returned. If an item exists but cannot be parsed
     * or is malformed/unrecognized, null is returned.
     *
     * @param {type} key
     */
    public getItem<T>(key: string): null | T {
        const entry: SessionStorageEntry<T> | null = this.getEntry(key);
        if (entry === null) {
            return null;
        }

        // if the entry has the specified field return its value
        if (entry['item']) {
            return entry['item'];
        } else {
            return null;
        }
    }

    /**
     * Removes the item with the specified key.
     *
     * @param {string} key
     */
    public removeItem(key: string): void {
        sessionStorage.removeItem(key);
    }
}
