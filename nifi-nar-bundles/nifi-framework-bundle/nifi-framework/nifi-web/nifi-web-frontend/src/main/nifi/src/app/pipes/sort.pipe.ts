/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Pipe, PipeTransform } from '@angular/core';

type Direction = 'asc' | 'desc';

@Pipe({
    name: 'sort'
})
export class SortPipe implements PipeTransform {
    transform(array: string[], direction: Direction = 'asc'): string[] {
        if (!array || array.length === 0) {
            return [];
        }
        return array.slice().sort((a, b) => {
            const isAsc = direction === 'asc';
            const retVal = this.compareString(a, b);
            return retVal * (isAsc ? 1 : -1);
        });
    }

    private compareString(a: string, b: string): number {
        if (a === b) {
            return 0;
        }
        return a < b ? -1 : 1;
    }
}
