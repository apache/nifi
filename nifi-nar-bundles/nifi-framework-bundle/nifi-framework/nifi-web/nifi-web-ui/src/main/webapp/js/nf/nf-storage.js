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

/* global nf, d3 */

nf.Storage = (function () {

    // Store items for two days before being eligible for removal.
    var TWO_DAYS = 86400000 * 2;

    return {
        /**
         * Initializes the storage. Items will be persisted for two days. Once the scripts runs
         * thereafter, all eligible items will be removed. This strategy does not support persistence.
         */
        init: function () {
            for (var i = 0; i < localStorage.length; i++) {
                try {
                    // get the next item
                    var key = localStorage.key(i);
                    var entry = JSON.parse(localStorage.getItem(key));

                    // get the expiration
                    var expires = new Date(entry.expires);
                    var now = new Date();

                    // if the expiration date has passed, remove it
                    if (expires.valueOf() < now.valueOf()) {
                        localStorage.removeItem(key);
                    }
                } catch (e) {
                    // likely unable to parse the item
                }
            }
        },
        
        /**
         * Stores the specified item.
         * 
         * @param {type} key
         * @param {type} item
         */
        setItem: function (key, item) {
            // calculate the expiration
            var expires = new Date().valueOf() + TWO_DAYS;

            // create the entry
            var entry = {
                expires: expires,
                item: item
            };

            // store the item
            localStorage.setItem(key, JSON.stringify(entry));
        },
        
        /**
         * Gets the item with the specified key. If an item with this key does
         * not exist, null is returned. If an item exists but cannot be parsed
         * or is malformed/unrecognized, null is returned.
         * 
         * @param {type} key
         */
        getItem: function (key) {
            try {
                // parse the entry
                var entry = JSON.parse(localStorage.getItem(key));

                // ensure the entry and item are present
                if (nf.Common.isDefinedAndNotNull(entry) && nf.Common.isDefinedAndNotNull(entry.item)) {
                    return entry.item;
                } else {
                    return null;
                }
            } catch (e) {
                return null;
            }
        },
        
        /**
         * Removes the item with the specified key.
         * 
         * @param {type} key
         */
        removeItem: function (key) {
            localStorage.removeItem(key);
        }
    };
}());