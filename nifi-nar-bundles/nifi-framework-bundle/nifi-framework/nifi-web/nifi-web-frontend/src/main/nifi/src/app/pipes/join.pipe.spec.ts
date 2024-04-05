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

import { JoinPipe } from './join.pipe';

describe('JoinPipe', () => {
    const values: string[] = ['alpha', 'omega', 'beta', 'theta', 'phi'];
    const numbers: number[] = [1, 2, 3];

    it('create an instance', () => {
        const pipe = new JoinPipe();
        expect(pipe).toBeTruthy();
    });

    it('should join by comma-space by default', () => {
        const pipe = new JoinPipe();
        const joined = pipe.transform(values);
        expect(joined).toEqual('alpha, omega, beta, theta, phi');
    });

    it('should join by a specified separator', () => {
        const pipe = new JoinPipe();
        const joined = pipe.transform(values, '-');
        expect(joined).toEqual('alpha-omega-beta-theta-phi');
    });

    it('should join numbers', () => {
        const pipe = new JoinPipe();
        const joined = pipe.transform(numbers, ' | ');
        expect(joined).toEqual('1 | 2 | 3');
    });
});
