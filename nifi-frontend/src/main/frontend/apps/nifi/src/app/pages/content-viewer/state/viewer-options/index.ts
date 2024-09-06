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

export const viewerOptionsFeatureKey = 'viewerOptions';

export const HEX_VIEWER_URL: string = '/content-viewer/hex-viewer';
export const BUNDLED_HEX_VIEWER: ContentViewer = {
    displayName: 'Bundled Hex Viewer',
    uri: HEX_VIEWER_URL,
    supportedMimeTypes: [
        {
            displayName: 'hex',
            mimeTypes: ['*/*']
        }
    ]
};

export const IMAGE_VIEWER_URL: string = '/content-viewer/image-viewer';
export const BUNDLED_IMAGE_VIEWER: ContentViewer = {
    displayName: 'Bundled Image Viewer',
    uri: IMAGE_VIEWER_URL,
    supportedMimeTypes: [
        {
            displayName: 'image',
            mimeTypes: ['image/png', 'image/jpeg', 'image/gif', 'image/webp']
        }
    ]
};

export interface SupportedMimeTypes {
    displayName: string;
    mimeTypes: string[];
}

export interface ContentViewer {
    displayName: string;
    uri: string;
    supportedMimeTypes: SupportedMimeTypes[];
}

export interface ContentViewerEntity {
    contentViewers: ContentViewer[];
}

export interface ViewerOptionsState {
    contentViewers: ContentViewer[] | null;
    bundledContentViewers: ContentViewer[];
    status: 'pending' | 'loading' | 'success';
}
