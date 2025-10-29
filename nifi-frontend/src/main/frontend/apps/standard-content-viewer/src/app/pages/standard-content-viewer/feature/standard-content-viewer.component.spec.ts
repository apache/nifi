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

// Mock CodeMirror dependencies
jest.mock('@codemirror/state', () => ({
    EditorState: {
        create: jest.fn().mockReturnValue({
            mockState: true,
            doc: {
                length: 0,
                lines: 1,
                toString: jest.fn().mockReturnValue('')
            }
        }),
        allowMultipleSelections: {
            of: jest.fn().mockReturnValue({ mockAllowMultipleSelections: true })
        },
        readOnly: {
            of: jest.fn().mockReturnValue({ mockReadOnly: true })
        }
    },
    Prec: {
        highest: jest.fn().mockReturnValue({ mockPrecHighest: true })
    },
    Annotation: {
        define: jest.fn().mockReturnValue({
            of: jest.fn().mockReturnValue({ mockAnnotation: true })
        })
    },
    Compartment: jest.fn().mockImplementation(() => ({
        of: jest.fn().mockReturnValue({ mockCompartment: true }),
        reconfigure: jest.fn().mockReturnValue({ mockReconfigure: true })
    }))
}));

jest.mock('@codemirror/view', () => ({
    EditorView: Object.assign(
        jest.fn().mockImplementation(() => ({
            contentDOM: {
                addEventListener: jest.fn(),
                removeEventListener: jest.fn()
            },
            state: {
                doc: {
                    length: 10,
                    lines: 2,
                    toString: jest.fn().mockReturnValue('test content')
                }
            },
            dispatch: jest.fn(),
            focus: jest.fn(),
            destroy: jest.fn()
        })),
        {
            lineWrapping: { mockLineWrapping: true },
            contentAttributes: {
                of: jest.fn().mockReturnValue({ mockContentAttributes: true })
            },
            theme: jest.fn().mockReturnValue({ mockTheme: true })
        }
    ),
    lineNumbers: jest.fn().mockReturnValue({ mockLineNumbers: true }),
    highlightActiveLine: jest.fn().mockReturnValue({ mockHighlightActiveLine: true }),
    highlightActiveLineGutter: jest.fn().mockReturnValue({ mockHighlightActiveLineGutter: true }),
    rectangularSelection: jest.fn().mockReturnValue({ mockRectangularSelection: true }),
    crosshairCursor: jest.fn().mockReturnValue({ mockCrosshairCursor: true }),
    keymap: {
        of: jest.fn().mockReturnValue({ mockKeymap: true })
    }
}));

jest.mock('@codemirror/language', () => ({
    syntaxHighlighting: jest.fn().mockReturnValue({ mockSyntaxHighlighting: true }),
    indentOnInput: jest.fn().mockReturnValue({ mockIndentOnInput: true }),
    bracketMatching: jest.fn().mockReturnValue({ mockBracketMatching: true }),
    foldGutter: jest.fn().mockReturnValue({ mockFoldGutter: true }),
    foldKeymap: [{ mockFoldKeymap: true }],
    indentUnit: {
        of: jest.fn().mockReturnValue({ mockIndentUnit: true })
    },
    HighlightStyle: {
        define: jest.fn().mockReturnValue({ mockHighlightStyle: true })
    }
}));

jest.mock('@codemirror/commands', () => ({
    history: jest.fn().mockReturnValue({ mockHistory: true }),
    defaultKeymap: [{ mockDefaultKeymap: true }],
    historyKeymap: [{ mockHistoryKeymap: true }]
}));

jest.mock('@codemirror/lang-json', () => ({
    json: jest.fn().mockReturnValue({ mockJson: true })
}));

jest.mock('@codemirror/lang-xml', () => ({
    xml: jest.fn().mockReturnValue({ mockXml: true })
}));

jest.mock('@codemirror/lang-yaml', () => ({
    yaml: jest.fn().mockReturnValue({ mockYaml: true })
}));

jest.mock('@codemirror/lang-markdown', () => ({
    markdown: jest.fn().mockReturnValue({ mockMarkdown: true })
}));

jest.mock('@codemirror/autocomplete', () => ({
    CompletionContext: jest.fn(),
    autocompletion: jest.fn().mockReturnValue({ mockAutocompletion: true }),
    Completion: jest.fn()
}));

// Mock @lezer/highlight
jest.mock('@lezer/highlight', () => ({
    tags: {
        keyword: 'keyword',
        string: 'string',
        comment: 'comment',
        number: 'number',
        operator: 'operator',
        punctuation: 'punctuation',
        bracket: 'bracket',
        variableName: 'variableName',
        function: jest.fn().mockReturnValue('function'),
        special: jest.fn().mockReturnValue('special'),
        definition: jest.fn().mockReturnValue('definition'),
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
jest.mock('@nifi/shared', () => ({
    ...jest.requireActual('@nifi/shared'),
    jsonHighlightStyle: { mockJsonHighlightStyle: true },
    xmlHighlightStyle: { mockXmlHighlightStyle: true },
    yamlHighlightStyle: { mockYamlHighlightStyle: true }
}));

describe('StandardContentViewer', () => {
    let component: StandardContentViewer;
    let fixture: ComponentFixture<StandardContentViewer>;
    let contentViewerService: jest.Mocked<ContentViewerService>;

    beforeEach(() => {
        const contentViewerServiceSpy = {
            getContent: jest.fn().mockReturnValue(of('mock content'))
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
        contentViewerService = TestBed.inject(ContentViewerService) as jest.Mocked<ContentViewerService>;

        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('Syntax Highlighting', () => {
        beforeEach(() => {
            // Clear mocks before each test
            jest.clearAllMocks();
        });

        it('should apply jsonHighlightStyle for json MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { json } = jest.requireMock('@codemirror/lang-json');

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
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { json } = jest.requireMock('@codemirror/lang-json');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'avro';

            component.loadContent();

            expect(json).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockJsonHighlightStyle: true });
        });

        it('should apply xmlHighlightStyle for xml MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { xml } = jest.requireMock('@codemirror/lang-xml');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'xml';

            component.loadContent();

            expect(xml).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockXmlHighlightStyle: true });
        });

        it('should apply yamlHighlightStyle for yaml MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { yaml } = jest.requireMock('@codemirror/lang-yaml');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'yaml';

            component.loadContent();

            expect(yaml).toHaveBeenCalled();
            expect(syntaxHighlighting).toHaveBeenCalledWith({ mockYamlHighlightStyle: true });
        });

        it('should apply markdown language for markdown MIME type without custom highlighting', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { markdown } = jest.requireMock('@codemirror/lang-markdown');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'markdown';

            component.loadContent();

            expect(markdown).toHaveBeenCalled();
            // Markdown doesn't use custom syntax highlighting
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for text MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { json } = jest.requireMock('@codemirror/lang-json');
            const { xml } = jest.requireMock('@codemirror/lang-xml');
            const { yaml } = jest.requireMock('@codemirror/lang-yaml');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'text';

            component.loadContent();

            expect(json).not.toHaveBeenCalled();
            expect(xml).not.toHaveBeenCalled();
            expect(yaml).not.toHaveBeenCalled();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for csv MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { json } = jest.requireMock('@codemirror/lang-json');
            const { xml } = jest.requireMock('@codemirror/lang-xml');
            const { yaml } = jest.requireMock('@codemirror/lang-yaml');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'csv';

            component.loadContent();

            expect(json).not.toHaveBeenCalled();
            expect(xml).not.toHaveBeenCalled();
            expect(yaml).not.toHaveBeenCalled();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });

        it('should not apply language-specific highlighting for unknown MIME type', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');
            const { json } = jest.requireMock('@codemirror/lang-json');
            const { xml } = jest.requireMock('@codemirror/lang-xml');
            const { yaml } = jest.requireMock('@codemirror/lang-yaml');

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
        it('should include base extensions for all MIME types', () => {
            const {
                lineNumbers,
                rectangularSelection,
                crosshairCursor,
                highlightActiveLine,
                highlightActiveLineGutter
            } = jest.requireMock('@codemirror/view');
            const { history } = jest.requireMock('@codemirror/commands');
            const { indentOnInput, bracketMatching, indentUnit } = jest.requireMock('@codemirror/language');
            const { EditorState, Prec } = jest.requireMock('@codemirror/state');

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
            const { foldGutter } = jest.requireMock('@codemirror/language');

            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = 'json';

            component.loadContent();

            expect(foldGutter).toHaveBeenCalled();
        });

        it('should not call loadContent when ref or mimeTypeDisplayName is missing', () => {
            const { syntaxHighlighting } = jest.requireMock('@codemirror/language');

            // Test with missing ref
            (component as any).ref = '';
            (component as any).mimeTypeDisplayName = 'json';
            component.loadContent();
            expect(syntaxHighlighting).not.toHaveBeenCalled();

            jest.clearAllMocks();

            // Test with missing mimeTypeDisplayName
            (component as any).ref = 'test-ref';
            (component as any).mimeTypeDisplayName = '';
            component.loadContent();
            expect(syntaxHighlighting).not.toHaveBeenCalled();
        });
    });
});
