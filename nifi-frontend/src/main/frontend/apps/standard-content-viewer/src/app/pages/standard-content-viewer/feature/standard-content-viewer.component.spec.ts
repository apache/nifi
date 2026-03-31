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

import { StandardContentViewer } from './standard-content-viewer.component';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideMockStore } from '@ngrx/store/testing';
import { DEFAULT_ROUTER_FEATURENAME } from '@ngrx/router-store';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { ReactiveFormsModule } from '@angular/forms';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { ContentViewerService } from '../service/content-viewer.service';
import { of } from 'rxjs';
import { syntaxHighlighting, indentOnInput, bracketMatching, foldGutter, indentUnit } from '@codemirror/language';
import { json } from '@codemirror/lang-json';
import { xml } from '@codemirror/lang-xml';
import { yaml } from '@codemirror/lang-yaml';
import { markdown } from '@codemirror/lang-markdown';
import {
    lineNumbers,
    rectangularSelection,
    crosshairCursor,
    highlightActiveLine,
    highlightActiveLineGutter
} from '@codemirror/view';
import { history } from '@codemirror/commands';
import { EditorState } from '@codemirror/state';

// Mock CodeMirror dependencies
vi.mock('@codemirror/state', () => ({
    EditorState: {
        create: vi.fn().mockReturnValue({
            mockState: true,
            doc: {
                length: 0,
                lines: 1,
                toString: vi.fn().mockReturnValue('')
            }
        }),
        allowMultipleSelections: {
            of: vi.fn().mockReturnValue({ mockAllowMultipleSelections: true })
        },
        readOnly: {
            of: vi.fn().mockReturnValue({ mockReadOnly: true })
        }
    },
    Prec: {
        highest: vi.fn().mockReturnValue({ mockPrecHighest: true })
    },
    Annotation: {
        define: vi.fn().mockReturnValue({
            of: vi.fn().mockReturnValue({ mockAnnotation: true })
        })
    },
    Compartment: vi.fn().mockImplementation(() => ({
        of: vi.fn().mockReturnValue({ mockCompartment: true }),
        reconfigure: vi.fn().mockReturnValue({ mockReconfigure: true })
    }))
}));

vi.mock('@codemirror/view', () => ({
    EditorView: Object.assign(
        vi.fn().mockImplementation(() => ({
            contentDOM: {
                addEventListener: vi.fn(),
                removeEventListener: vi.fn()
            },
            state: {
                doc: {
                    length: 10,
                    lines: 2,
                    toString: vi.fn().mockReturnValue('test content')
                }
            },
            dispatch: vi.fn(),
            focus: vi.fn(),
            destroy: vi.fn()
        })),
        {
            lineWrapping: { mockLineWrapping: true },
            contentAttributes: {
                of: vi.fn().mockReturnValue({ mockContentAttributes: true })
            },
            theme: vi.fn().mockReturnValue({ mockTheme: true })
        }
    ),
    lineNumbers: vi.fn().mockReturnValue({ mockLineNumbers: true }),
    highlightActiveLine: vi.fn().mockReturnValue({ mockHighlightActiveLine: true }),
    highlightActiveLineGutter: vi.fn().mockReturnValue({ mockHighlightActiveLineGutter: true }),
    rectangularSelection: vi.fn().mockReturnValue({ mockRectangularSelection: true }),
    crosshairCursor: vi.fn().mockReturnValue({ mockCrosshairCursor: true }),
    keymap: {
        of: vi.fn().mockReturnValue({ mockKeymap: true })
    }
}));

vi.mock('@codemirror/language', () => ({
    syntaxHighlighting: vi.fn().mockReturnValue({ mockSyntaxHighlighting: true }),
    indentOnInput: vi.fn().mockReturnValue({ mockIndentOnInput: true }),
    bracketMatching: vi.fn().mockReturnValue({ mockBracketMatching: true }),
    foldGutter: vi.fn().mockReturnValue({ mockFoldGutter: true }),
    foldKeymap: [{ mockFoldKeymap: true }],
    indentUnit: {
        of: vi.fn().mockReturnValue({ mockIndentUnit: true })
    },
    HighlightStyle: {
        define: vi.fn().mockReturnValue({ mockHighlightStyle: true })
    }
}));

vi.mock('@codemirror/commands', () => ({
    history: vi.fn().mockReturnValue({ mockHistory: true }),
    defaultKeymap: [{ mockDefaultKeymap: true }],
    historyKeymap: [{ mockHistoryKeymap: true }]
}));

vi.mock('@codemirror/lang-json', () => ({
    json: vi.fn().mockReturnValue({ mockJson: true })
}));

vi.mock('@codemirror/lang-xml', () => ({
    xml: vi.fn().mockReturnValue({ mockXml: true })
}));

vi.mock('@codemirror/lang-yaml', () => ({
    yaml: vi.fn().mockReturnValue({ mockYaml: true })
}));

vi.mock('@codemirror/lang-markdown', () => ({
    markdown: vi.fn().mockReturnValue({ mockMarkdown: true })
}));

vi.mock('@codemirror/autocomplete', () => ({
    CompletionContext: vi.fn(),
    autocompletion: vi.fn().mockReturnValue({ mockAutocompletion: true }),
    Completion: vi.fn()
}));

// Mock @lezer/highlight
vi.mock('@lezer/highlight', () => ({
    tags: {
        keyword: 'keyword',
        string: 'string',
        comment: 'comment',
        number: 'number',
        operator: 'operator',
        punctuation: 'punctuation',
        bracket: 'bracket',
        variableName: 'variableName',
        function: vi.fn().mockReturnValue('function'),
        special: vi.fn().mockReturnValue('special'),
        definition: vi.fn().mockReturnValue('definition'),
        labelName: 'labelName',
        typeName: 'typeName',
        className: 'className',
        changed: 'changed',
        annotation: 'annotation',
        modifier: 'modifier',
        self: 'self',
        namespace: 'namespace',
        operatorKeyword: 'operatorKeyword',
        url: 'url',
        escape: 'escape',
        regexp: 'regexp',
        link: 'link',
        heading: 'heading',
        atom: 'atom',
        bool: 'bool',
        processingInstruction: 'processingInstruction',
        inserted: 'inserted',
        invalid: 'invalid',
        name: 'name',
        separator: 'separator',
        tagName: 'tagName',
        attributeName: 'attributeName',
        attributeValue: 'attributeValue',
        angleBracket: 'angleBracket',
        squareBracket: 'squareBracket',
        content: 'content',
        meta: 'meta',
        brace: 'brace',
        propertyName: 'propertyName'
    }
}));

// Mock the highlight styles
vi.mock('@nifi/shared', async () => ({
    ...(await vi.importActual('@nifi/shared')),
    jsonHighlightStyle: { mockJsonHighlightStyle: true },
    xmlHighlightStyle: { mockXmlHighlightStyle: true },
    yamlHighlightStyle: { mockYamlHighlightStyle: true }
}));

describe('StandardContentViewer', () => {
    let component: StandardContentViewer;
    let fixture: ComponentFixture<StandardContentViewer>;
    let _contentViewerService: vi.Mocked<ContentViewerService>;

    beforeEach(() => {
        const contentViewerServiceSpy = {
            getContent: vi.fn().mockReturnValue(of('mock content'))
        };

        TestBed.configureTestingModule({
            declarations: [StandardContentViewer],
            imports: [HttpClientTestingModule, MatButtonToggleModule, ReactiveFormsModule, NgxSkeletonLoaderModule],
            providers: [
                provideMockStore({
                    initialState: {
                        [DEFAULT_ROUTER_FEATURENAME]: {
                            state: {
                                queryParams: {}
                            }
                        }
                    }
                }),
                { provide: ContentViewerService, useValue: contentViewerServiceSpy }
            ]
        });
        fixture = TestBed.createComponent(StandardContentViewer);
        component = fixture.componentInstance;
        _contentViewerService = TestBed.inject(ContentViewerService) as vi.Mocked<ContentViewerService>;

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('Syntax Highlighting', () => {
        beforeEach(() => {
            // Clear mocks before each test
            vi.clearAllMocks();
        });

        it('should apply jsonHighlightStyle for json MIME type', () => {
            // Set up component properties using bracket notation to access private properties
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'json';

            // Call loadContent
            component.loadContent();

            // Verify json language and jsonHighlightStyle are used
            expect(json).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockJsonHighlightStyle: true });
        });

        it('should apply jsonHighlightStyle for avro MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'avro';

            component.loadContent();

            expect(json).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockJsonHighlightStyle: true });
        });

        it('should apply xmlHighlightStyle for xml MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'xml';

            component.loadContent();

            expect(xml).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockXmlHighlightStyle: true });
        });

        it('should apply yamlHighlightStyle for yaml MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'yaml';

            component.loadContent();

            expect(yaml).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockYamlHighlightStyle: true });
        });

        it('should apply markdown language for markdown MIME type without custom highlighting', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'markdown';

            component.loadContent();

            expect(markdown).toHaveBeenCalled();
            // Markdown doesn't use custom syntax highlighting
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for text MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'text';

            component.loadContent();

            expect(json).not.toHaveBeenCalled();
            expect(xml).not.toHaveBeenCalled();
            expect(yaml).not.toHaveBeenCalled();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for csv MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'csv';

            component.loadContent();

            expect(json).not.toHaveBeenCalled();
            expect(xml).not.toHaveBeenCalled();
            expect(yaml).not.toHaveBeenCalled();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for unknown MIME type', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'unknown';

            component.loadContent();

            expect(json).not.toHaveBeenCalled();
            expect(xml).not.toHaveBeenCalled();
            expect(yaml).not.toHaveBeenCalled();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });
    });

    describe('CodeMirror Configuration', () => {
        beforeEach(() => {
            vi.clearAllMocks();
        });

        it('should include base extensions for all MIME types', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'json';

            component.loadContent();

            // Verify base extensions are included
            expect(lineNumbers).toHaveBeenCalled();
            expect(history).toHaveBeenCalled();
            expect(rectangularSelection).toHaveBeenCalled();
            expect(crosshairCursor).toHaveBeenCalled();
            expect(highlightActiveLine).toHaveBeenCalled();
            expect(highlightActiveLineGutter).toHaveBeenCalled();
            expect(indentOnInput).toHaveBeenCalled();
            expect(bracketMatching).toHaveBeenCalled();
            expect(indentUnit.of).toHaveBeenCalledWith('    ');
            expect(EditorState.allowMultipleSelections.of).toHaveBeenCalledWith(true);
        });

        it('should include fold functionality for structured languages', () => {
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'json';

            component.loadContent();

            expect(foldGutter).toHaveBeenCalled();
        });

        it('should not call loadContent when ref or mimeTypeDisplayName is missing', () => {
            // Test with missing ref
            (component as any).ref = '';
            (component as any).mimeTypeDisplayName = 'json';
            component.loadContent();
            expect(syntaxHighlighting).not.toHaveBeenCalled();

            vi.clearAllMocks();

            // Test with missing mimeTypeDisplayName
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = '';
            component.loadContent();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });
    });
});
