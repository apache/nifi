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

/* global nf */

/**
 * jQuery plugin for a NiFi style tag cloud. The options are specified in the following
 * format:
 *
 * {
 *   tags: ['attributes', 'copy', 'regex', 'xml', 'copy', 'xml', 'attributes'],
 *   select: selectHandler,
 *   remove: removeHandler
 * }
 *
 * @param {type} $
 * @returns {undefined}
 */
(function ($) {

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

    var config = {
        maxTags: 25,
        maxTagFontSize: 2,
        minTagFontSize: 1,
        minWidth: 20
    };

    /**
     * Adds the specified tag filter.
     *
     * @argument {jQuery} cloudContainer    The tag cloud container
     * @argument {string} tag               The tag to add
     */
    var addTagFilter = function (cloudContainer, tag) {
        var config = cloudContainer.data('options');
        var tagFilter = cloudContainer.find('ul.tag-filter');

        // ensure this tag hasn't already been added
        var tagFilterExists = false;
        tagFilter.find('li div.selected-tag-text').each(function () {
            if (tag === $(this).text()) {
                tagFilterExists = true;
                return false;
            }
        });

        // add this tag filter if applicable
        if (!tagFilterExists) {
            // create the list item content
            var tagText = $('<div class="selected-tag-text"></div>').text(tag);
            var removeTagIcon = $('<div class="fa fa-close remove-selected-tag pointer"></div>').click(function () {
                // remove this tag
                $(this).closest('li').remove();

                // fire a remove event if applicable
                if (isDefinedAndNotNull(config.remove)) {
                    config.remove.call(cloudContainer, tag);
                }
            });
            var selectedTagItem = $('<div layout="row" layout-align="space-between center"></div>').append(tagText).append(removeTagIcon);

            // create the list item and update the tag filter list
            $('<li></li>').append(selectedTagItem).appendTo(tagFilter);

            // fire a select event if applicable
            if (isDefinedAndNotNull(config.select)) {
                config.select.call(cloudContainer, tag);
            }
        }
    };

    var methods = {

        /**
         * Initializes the tag cloud.
         *
         * @argument {object} options The options for the tag cloud
         */
        init: function (options) {
            return this.each(function () {
                // ensure the options have been properly specified
                if (isDefinedAndNotNull(options) && isDefinedAndNotNull(options.tags)) {
                    // get the tag cloud
                    var cloudContainer = $(this);

                    // clear any current contents, remote events, and store options
                    cloudContainer.empty().unbind().data('options', options);

                    // build the component
                    var cloud = $('<ul class="tag-cloud"></ul>').appendTo(cloudContainer);
                    var filter = $('<ul class="tag-filter"></ul>').appendTo(cloudContainer);

                    var tagCloud = {};
                    var tags = [];

                    // count the frequency of each tag for this type
                    $.each(options.tags, function (i, tag) {
                        var normalizedTagName = tag.toLowerCase();

                        if (isDefinedAndNotNull(tagCloud[normalizedTagName])) {
                            tagCloud[normalizedTagName].count = tagCloud[normalizedTagName].count + 1;
                        } else {
                            var tagCloudEntry = {
                                term: normalizedTagName,
                                count: 1
                            };
                            tags.push(tagCloudEntry);
                            tagCloud[normalizedTagName] = tagCloudEntry;
                        }
                    });

                    // handle the case when no tags are present
                    if (tags.length > 0) {
                        // sort the tags by frequency to limit the less frequent tags
                        tags.sort(function (a, b) {
                            return b.count - a.count;
                        });

                        // limit to the most frequest tags
                        if (tags.length > config.maxTags) {
                            tags = tags.slice(0, config.maxTags);
                        }

                        // determine the max frequency
                        var maxFrequency = tags[0].count;

                        // sort the tags alphabetically
                        tags.sort(function (a, b) {
                            var compA = a.term.toUpperCase();
                            var compB = b.term.toUpperCase();
                            return (compA < compB) ? -1 : (compA > compB) ? 1 : 0;
                        });

                        // set the tag content
                        $.each(tags, function (i, tag) {
                            // determine the appropriate font size
                            var fontSize = Math.log(tag.count) / Math.log(maxFrequency) * (config.maxTagFontSize - config.minTagFontSize) + config.minTagFontSize;
                            var minWidth = config.minWidth * fontSize;

                            // create the tag cloud entry
                            $('<li></li>').append($('<span class="link"></span>').text(tag.term).css({
                                'font-size': fontSize + 'em'
                            })).css({
                                'min-width': minWidth + 'px'
                            }).click(function () {
                                // ensure we don't exceed 5 selected
                                if (filter.children('li').length < 5) {
                                    var tagText = $(this).children('span').text();
                                    addTagFilter(cloudContainer, tagText);
                                }
                            }).appendTo(cloud).ellipsis();
                        });
                    } else {
                        // indicate when no tags are found
                        $('<li><span class="unset">No tags specified</span></li>').appendTo(cloud);
                    }
                }
            });
        },

        /**
         * Resets the selected tags from the tag cloud.
         */
        clearSelectedTags: function () {
            return this.each(function () {
                var cloudContainer = $(this);
                cloudContainer.find('div.remove-selected-tag').trigger('click');
            });
        },

        /**
         * Returns the selected tags of the first matching element.
         */
        getSelectedTags: function () {
            var tags = [];

            this.each(function () {
                var cloudContainer = $(this);
                cloudContainer.find('div.selected-tag-text').each(function () {
                    tags.push($(this).text());
                });
            });

            return tags;
        }
    };

    $.fn.tagcloud = function (method) {
        if (methods[method]) {
            return methods[method].apply(this, Array.prototype.slice.call(arguments, 1));
        } else {
            return methods.init.apply(this, arguments);
        }
    };
})(jQuery);