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

    /**
     * Create a new tabbed pane. The options are specified in the following
     * format:
     *
     * {
     *   tabStyle: 'tabStyle',
     *   selectedTabStyle: 'selectedTabStyle',
     *   tabs: [{
     *      name: 'tab 1',
     *      tabContentId: 'tab1ContentId'
     *   }, {
     *      name: 'tab 2',
     *      tabContentId: 'tab2ContentId'
     *   }],
     *   select: selectHandler
     * }
     *
     * @argument {object} options
     */
    $.fn.tabbs = function (options) {
        return this.each(function () {
            var tabPane = $(this);

            // create the unorder list
            var tabList = $('<ul/>').addClass('tab-pane');
            var shownTab = false;

            // create each tab
            $.each(options.tabs, function () {
                // generate the tab content style
                var tabContentStyle = tabPane.attr('id') + '-content';
                var tabDefinition = this;

                // mark the tab content
                $('#' + tabDefinition.tabContentId).addClass(tabContentStyle).hide();

                // prep the tab itself
                var tab = $('<li></li>').text(tabDefinition.name).addClass(options.tabStyle).click(function () {
                    // hide all tab content
                    $('.' + tabContentStyle).hide();

                    // show the desired tab
                    var tabContent = $('#' + tabDefinition.tabContentId).show();

                    // check if content has overflow
                    var tabContentContainer = tabContent.parent();
                    if (tabContentContainer.get(0).offsetHeight < tabContentContainer.get(0).scrollHeight ||
                        tabContentContainer.get(0).offsetWidth < tabContentContainer.get(0).scrollWidth) {
                        tabContentContainer.addClass(options.scrollableTabContentStyle);
                    } else {
                        tabContentContainer.removeClass(options.scrollableTabContentStyle);
                    }

                    // unselect the previously selected tab
                    tabList.children('.' + options.tabStyle).removeClass(options.selectedTabStyle);

                    // mark the tab as selected
                    $(this).addClass(options.selectedTabStyle);

                    // fire a select event if applicable
                    if (isDefinedAndNotNull(options.select)) {
                        options.select.call(this);
                    }
                }).appendTo(tabList);

                // always show the first tab
                if (!shownTab) {
                    tab.click();
                    shownTab = true;
                }
            });

            // add the tab list
            tabList.appendTo(tabPane);
        });
    };
})(jQuery);