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

interface StorageEntry<T> {
    expires: number;
    item: T;
}

@Injectable({
    providedIn: 'root'
})
export class Storage {
    private static readonly MILLIS_PER_DAY: number = 86400000;
    private static readonly TWO_DAYS: number = Storage.MILLIS_PER_DAY * 2;

    constructor() {
        for (let i = 0; i < localStorage.length; i++) {
            try {
                // get the next item
                const key: string | null = localStorage.key(i);

                if (key) {
                    // attempt to get the item which will expire if necessary
                    this.getItem(key);
                }
            } catch (e) {
                //do nothing
            }
        }
    }

    /**
     * Checks the expiration for the specified entry.
     *
     * @param {object} entry
     * @returns {boolean}
     */
    private checkExpiration<T>(entry: StorageEntry<T>): boolean {
        if (entry.expires) {
            // get the expiration
            const expires: Date = new Date(entry.expires);
            const now: Date = new Date();

            // return whether the expiration date has passed
            return expires.valueOf() < now.valueOf();
        } else {
            return false;
        }
    }

    /**
     * Gets an entry for the key. The entry expiration is not checked.
     *
     * @param {string} key
     */
    private getEntry<T>(key: string): null | StorageEntry<T> {
        try {
            // parse the entry
            const item = localStorage.getItem(key);
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
     * @param {number} expires
     */
    public setItem<T>(key: string, item: T, expires?: number): void {
        // calculate the expiration
        expires = expires != null ? expires : new Date().valueOf() + Storage.TWO_DAYS;

        // create the entry
        const entry: StorageEntry<T> = {
            expires,
            item
        };

        // store the item
        localStorage.setItem(key, JSON.stringify(entry));
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
        const entry: StorageEntry<T> | null = this.getEntry(key);
        if (entry === null) {
            return null;
        }

        // if the entry is expired, drop it and return null
        if (this.checkExpiration(entry)) {
            this.removeItem(key);
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
     * Gets the expiration for the specified item. This will not check the expiration. If
     * the entry is expired, it will return null on a subsequent getItem invocation.
     *
     * @param {string} key
     * @returns {number}
     */
    public getItemExpiration(key: string): null | number {
        const entry = this.getEntry(key);
        if (entry === null) {
            return null;
        }

        // if the entry has the specified field return its value
        if (entry['expires']) {
            return entry['expires'];
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
        localStorage.removeItem(key);
    }
}
