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
 * Create a new status bar.
 *
 *  $(selector).statusbar();
 *
 *
 */
(function ($) {

    // static key path variables
    var PROCESSOR_ID_KEY = 'component.id',
        ACTIVE_THREAD_COUNT_KEY = 'status.aggregateSnapshot.activeThreadCount',
        RUN_STATUS_KEY = 'status.aggregateSnapshot.runStatus',
        BULLETINS_KEY = 'bulletins';

    var isUndefined = function (obj) {
        return typeof obj === 'undefined';
    };

    var isNull = function (obj) {
        return obj === null;
    };

    var isDefinedAndNotNull = function (obj) {
        return !isUndefined(obj) && !isNull(obj);
    };

    var getKeyValue = function(obj,key){
        return key.split('.').reduce(function(o,x){
            return(typeof o === undefined || o === null)? o : (typeof o[x] == 'function')?o[x]():o[x];
        }, obj);
    };

    var methods = {

        /**
         * Initializes the status bar.
         */
        init: function () {
            // get the combo
            var bar = $(this).addClass('dialog-status-bar');

            bar.html('<text class="run-status-icon"></text>'+
                     '<span class="dialog-status-bar-state"></span>'+
                     '<span class="dialog-status-bar-threads" count="0"></span>'+
                     '<div class="dialog-status-bar-bulletins fa fa-sticky-note-o" count="0">'+
                         '<div class="dialog-status-bar-bulletins-content"></div>'+
                     '</div>'+
                     '<div class="dialog-status-bar-buttons"></div>');
            return bar;
        },

        /**
         * Shows the  status bar.
         */
        show: function () {
            var bar = $(this);
            if (bar.is(':visible')) {
                bar.show();
            }
            return bar;
        },

        /**
         * Hides the status bar.
         */
        hide: function () {
            var bar = $(this);
            if (bar.is(':visible')) {
                bar.hide();
            }
            return bar;
        },


        /**
         * Initializes the synchronization process to the canvas element
         *
         * @param id - id value of the processor to observe
         * @param cb - callback to execute when a mutation is detected
         */
        observe: function(id,cb) {
            var bar = $(this);
            var g = document.querySelector('g[id="id-'+id+'"]');

            //perform the initial set
            bar.statusbar('set',id);

            //create and store an observer
            bar.data('observer',new MutationObserver(function(mutations){
                bar.statusbar('set',id);
                if(typeof cb == 'function'){
                    cb();
                }
            }));

            //initialize the observer
            bar.data('observer').observe(g,{attributes:true,childList:true,subtree:true});

            return bar.data('observer');
        },

        /**
         * Terminates the synchronization process
         */
        disconnect: function () {
            var bar = $(this);
            if(isDefinedAndNotNull(bar.data('observer'))){
                bar.data('observer').disconnect();
                bar.data('observer',null);
            }
            if(isDefinedAndNotNull(bar.data('buttonModel'))){
                bar.data('buttonModel', []);
                bar.statusbar('refreshButtons',[]);
            }
        },

        /**
         * Refreshes the buttons with the existing button model.
         */
        refreshButtons: function () {
            var bar = $(this);
            bar.statusbar('buttons',bar.data('buttonModel'));
            return bar;
        },

        /**
         * Hides all buttons
         */
        hideButtons: function () {
            var bar = $(this);
            bar.find('.dialog-status-bar-buttons').hide();
            return bar;
        },

        /**
         * Shows all buttons.
         */
        showButtons: function () {
            var bar = $(this);
            bar.find('.dialog-status-bar-buttons').show(250);
            return bar;
        },

        /**
         * Sets/Retrieves the buttons on the status bar
         *
         * @param [{object}] button objects to apply
         */
        buttons : function(buttons){
            var bar = $(this),
                buttonWrapper = bar.find('.dialog-status-bar-buttons');

            if(isDefinedAndNotNull(buttons)){
                //remove any existing buttons
                buttonWrapper.children().remove();

                //add in new buttons
                $.each(buttons, function (i, buttonConfig) {
                    var isDisabled = function () {
                        return typeof buttonConfig.disabled === 'function' && buttonConfig.disabled.call() === true;
                    };

                    // create the button
                    var button = $('<div class="button"></div>');
                    if(buttonConfig.buttonText){
                        button.append($('<span></span>').text(buttonConfig.buttonText));
                    }
                    else if(buttonConfig.buttonHtml){
                        button.html(buttonConfig.buttonHtml);
                    }

                    // add the class if specified
                    if (isDefinedAndNotNull(buttonConfig.clazz)) {
                        button.addClass(buttonConfig.clazz);
                    }

                    // set the color if specified
                    if (isDefinedAndNotNull(buttonConfig.color)) {
                        button.css({
                            'background': buttonConfig.color.base,
                            'color': buttonConfig.color.text
                        });
                    }

                    // check if the button should be disabled
                    if (isDisabled()) {
                        button.addClass('disabled-button');
                    } else {
                        // enable custom hover if specified
                        if (isDefinedAndNotNull(buttonConfig.color)) {
                            button.hover(function () {
                                $(this).css("background-color", buttonConfig.color.hover);
                            }, function () {
                                $(this).css("background-color", buttonConfig.color.base);
                            });
                        }

                        button.click(function () {
                            var handler = $(this).data('handler');
                            if (isDefinedAndNotNull(handler) && typeof handler.click === 'function') {
                                handler.click.call(bar);
                            }
                        });
                    }

                    // add the button to the wrapper
                    button.data('handler', buttonConfig.handler).appendTo(buttonWrapper);
                });

                // store the button model to refresh later
                bar.data('buttonModel', buttons);
            }

            //return the buttons as an array object
            buttons = [];
            $.each(buttonWrapper.find('.button'),function(i, button){
                buttons.push($(button));
            });
            return buttons;
        },

        /**
         * Set the status bar display values
         *
         * @param id - processor id to evaluate
         */
        set : function(id) {
            var bar = $(this),
                obj = d3.select('#id-' + id).datum(),
                bulletinList = $("<ul></ul>"),
                runStatus = getKeyValue(obj,RUN_STATUS_KEY),
                activeThreadCount = getKeyValue(obj,ACTIVE_THREAD_COUNT_KEY),
                bulletins = getKeyValue(obj,BULLETINS_KEY);

            //set the values
            if(isDefinedAndNotNull(runStatus) &&
                isDefinedAndNotNull(activeThreadCount) &&
                isDefinedAndNotNull(bulletins) &&
                Array.isArray(bulletins)) {

                bar.attr('state',runStatus.toUpperCase());
                bar.find('.dialog-status-bar-state').text(runStatus);
                bar.find('.dialog-status-bar-threads').attr('count',activeThreadCount);
                bar.find('.dialog-status-bar-threads').attr('title',activeThreadCount+' active threads');
                bar.find('.dialog-status-bar-threads').text('('+activeThreadCount+')');
                $.each(bulletins, function(i,item){
                    if(item.canRead){
                       bulletinList.append($('<li>'+item.bulletin.timestamp+' '+item.bulletin.level+'<br/>'+item.bulletin.message+'<br>&nbsp;</li>'));
                    }
                });
                var bulletinCount = bulletinList.find('li').length;
                bar.find('.dialog-status-bar-bulletins-content').html((bulletinCount > 0)?bulletinList:'');
                bar.find('.dialog-status-bar-bulletins').attr('count',bulletinCount);

                //update the button state
                bar.statusbar('refreshButtons');
            }
            return bar;
        }
    };

    $.fn.statusbar = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };

})(jQuery);