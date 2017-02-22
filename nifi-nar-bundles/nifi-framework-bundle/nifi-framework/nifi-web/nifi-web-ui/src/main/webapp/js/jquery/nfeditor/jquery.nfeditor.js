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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery',
                'CodeMirror',
                'nf'],
            function ($, CodeMirror, nf) {
                factory($, CodeMirror, nf);
            });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        factory(require('jquery'),
            require('CodeMirror'),
            require('nf'));
    } else {
        factory(root.$,
            root.CodeMirror,
            root.nf);
    }
}(this, function ($, CodeMirror, nf) {

    var isUndefined = function (obj) {
        return typeof obj === 'undefined';
    };

    var isNull = function (obj) {
        return obj === null;
    };

    var isDefinedAndNotNull = function (obj) {
        return !isUndefined(obj) && !isNull(obj);
    };

    var isBlank = function (str) {
        return isUndefined(str) || isNull(str) || str === '';
    };

    var isFunction = function (funct) {
        return typeof funct === 'function';
    };

    var methods = {

        /**
         * Create a new nf editor. The options are specified in the following
         * format:
         *
         * {
         *   languageId: 'nfel',
         *   resizable: true,
         *   sensitive: false,
         *   readOnly: false,
         *   content: '${attribute}',
         *   width: 200,
         *   height: 200,
         *   minWidth: 150,
         *   minHeight: 150
         * }
         *
         * @param {object} options  The options for this editor.
         */
        init: function (options) {
            return this.each(function () {
                if (isUndefined(options) || isNull(options)) {
                    return false;
                }

                var languageId = options.languageId;
                var languageAssist = nf[languageId];

                if (isUndefined(languageAssist) || isNull(languageAssist)) {
                    return false;
                }

                // should support resizing
                var resizable = options.resizable === true;

                // is the property sensitive
                var sensitive = options.sensitive === true;

                var content = isDefinedAndNotNull(options.content) ? options.content : '';
                var field = $('<textarea></textarea>').text(content).appendTo($(this));

                // define a mode for NiFi expression language
                if (isFunction(languageAssist.color)) {
                    CodeMirror.commands.autocomplete = function (cm) {
                        if (isFunction(languageAssist.suggest)) {
                            CodeMirror.showHint(cm, nf[languageId].suggest);
                        }
                    };

                    CodeMirror.defineMode(languageId, nf[languageId].color);

                    // is the editor read only
                    var readOnly = options.readOnly === true;

                    var editor = CodeMirror.fromTextArea(field.get(0), {
                        mode: languageId,
                        lineNumbers: true,
                        matchBrackets: true,
                        readOnly: readOnly,
                        extraKeys: {
                            'Ctrl-Space': 'autocomplete',
                            'Esc': function (cm) {
                                if (isFunction(options.escape)) {
                                    options.escape();
                                }
                            },
                            'Enter': function (cm) {
                                if (isFunction(options.enter)) {
                                    options.enter();
                                }
                            }
                        }
                    });

                    // set the size
                    var width = null;
                    if (isDefinedAndNotNull(options.width)) {
                        width = options.width;
                    }
                    var height = null;
                    if (isDefinedAndNotNull(options.height)) {
                        height = options.height;
                    }
                    editor.setSize(width, height);

                    // store the editor instance for later
                    $(this).data('editorInstance', editor);

                    // get a reference to the codemirror
                    var codeMirror = $(this).find('.CodeMirror');

                    // reference the code portion
                    var code = codeMirror.find('.CodeMirror-code');

                    // make this resizable if specified
                    if (resizable) {
                        codeMirror.append('<div class="ui-resizable-handle ui-resizable-se"></div>').resizable({
                            handles: {
                                'se': '.ui-resizable-se'
                            },
                            resize: function () {
                                editor.setSize($(this).width(), $(this).height());
                                editor.refresh();
                            }
                        });
                    }

                    // handle keydown to signify the content has changed
                    editor.on('change', function (cm, event) {
                        codeMirror.addClass('modified');
                    });

                    // handle keyHandled to stop event propagation/default as necessary
                    editor.on('keyHandled', function (cm, name, evt) {
                        if (name === 'Esc') {
                            // stop propagation of the escape event
                            evt.stopImmediatePropagation();
                            evt.preventDefault();
                        }
                    });

                    // handle sensitive values differently
                    if (sensitive) {
                        code.addClass('sensitive');

                        var handleSensitive = function (cm, event) {
                            if (code.hasClass('sensitive')) {
                                code.removeClass('sensitive');
                                editor.setValue('');
                            }
                        };

                        // remove the sensitive style if necessary
                        editor.on('mousedown', handleSensitive);
                        editor.on('keydown', handleSensitive);
                    }

                    // set the min width/height
                    if (isDefinedAndNotNull(options.minWidth)) {
                        codeMirror.resizable('option', 'minWidth', options.minWidth);
                    }
                    if (isDefinedAndNotNull(options.minHeight)) {
                        codeMirror.resizable('option', 'minHeight', options.minHeight);
                    }
                }
            });
        },

        /**
         * Refreshes the editor.
         */
        refresh: function () {
            return this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    editor.refresh();
                }
            });
        },

        /**
         * Sets the size of the editor.
         *
         * @param {integer} width
         * @param {integer} height
         */
        setSize: function (width, height) {
            return this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    editor.setSize(width, height);
                }
            });
        },

        /**
         * Gets the value of the editor in the first matching selector.
         */
        getValue: function () {
            var value;

            this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    value = editor.getValue();
                }

                return false;
            });

            return value;
        },

        /**
         * Sets the value of the editor.
         *
         * @param {string} value
         */
        setValue: function (value) {
            return this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    editor.setValue(value);

                    // remove the modified marking since the value was reset
                    $(this).find('.CodeMirror').removeClass('modified');
                }
            });
        },

        /**
         * Sets the focus.
         */
        focus: function () {
            return this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    editor.focus();
                }
            });
        },

        /**
         * Sets the focus.
         */
        selectAll: function () {
            return this.each(function () {
                var editor = $(this).data('editorInstance');

                // ensure the editor was initialized
                if (isDefinedAndNotNull(editor)) {
                    editor.execCommand('selectAll');
                }
            });
        },

        /**
         * Gets whether the value of the editor in the first matching selector has been modified.
         */
        isModified: function () {
            var modified;

            this.each(function () {
                modified = $(this).find('.CodeMirror').hasClass('modified');
                return false;
            });

            return modified;
        },

        /**
         * Destroys the editor.
         */
        destroy: function () {
            return this.removeData('editorInstance').find('.CodeMirror').removeClass('modified');
        }
    };

    $.fn.nfeditor = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
}));