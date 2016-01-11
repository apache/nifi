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

/* global top */

$(document).ready(function () {

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
     * Get the filter text.
     * 
     * @returns {unresolved}
     */
    var getFilterText = function () {
        var filter = '';
        var ruleFilter = $('#component-filter');
        if (!ruleFilter.hasClass('component-filter-list')) {
            filter = ruleFilter.val();
        }
        return filter;
    };

    var applyComponentFilter = function (componentContainer) {
        var matchingComponents = 0;
        var componentLinks = $(componentContainer).find('a.component-link');

        if (componentLinks.length === 0) {
            return matchingComponents;
        }

        // get the filter text
        var filter = getFilterText();
        if (filter !== '') {
            var filterExp = new RegExp(filter, 'i');

            // update the displayed rule count
            $.each(componentLinks, function (_, componentLink) {
                var a = $(componentLink);
                var li = a.closest('li.component-item');

                // get the rule text for matching
                var componentName = a.text();

                // see if any of the text from this rule matches
                var componentMatches = componentName.search(filterExp) >= 0;

                // handle whether the rule matches
                if (componentMatches === true) {
                    li.show();
                    matchingComponents++;
                } else {
                    // hide the rule
                    li.hide();
                }
            });
        } else {
            // ensure every rule is visible
            componentLinks.closest('li.component-item').show();

            // set the number of displayed rules
            matchingComponents = componentLinks.length;
        }

        // show whether there are status if appropriate
        var noMatching = componentContainer.find('span.no-matching');
        if (matchingComponents === 0) {
            noMatching.show();
        } else {
            noMatching.hide();
        }

        return matchingComponents;
    };

    var applyFilter = function () {
        var matchingGeneral = applyComponentFilter($('#general-links'));
        var matchingProcessors = applyComponentFilter($('#processor-links'));
        var matchingControllerServices = applyComponentFilter($('#controller-service-links'));
        var matchingReportingTasks = applyComponentFilter($('#reporting-task-links'));
        var matchingDeveloper = applyComponentFilter($('#developer-links'));

        // update the rule count
        $('#displayed-components').text(matchingGeneral + matchingProcessors + matchingControllerServices + matchingReportingTasks + matchingDeveloper);
    };

    var selectComponent = function (componentName) {
        var componentLinks = $('a.component-link');

        // consider each link
        $.each(componentLinks, function () {
            var componentLink = $(this);
            if (componentName === componentLink.text()) {
                // remove all selected styles
                $('li.component-item').removeClass('selected');

                // select this links item
                componentLink.closest('li.component-item').addClass('selected');

                // set the header
                $('#selected-component').text(componentLink.text());

                // stop iteration
                return false;
            }
        });
    };

    // get the banners if we're not in the shell
    var banners = $.Deferred(function (deferred) {
        if (top === window) {
            $.ajax({
                type: 'GET',
                url: '../nifi-api/controller/banners',
                dataType: 'json'
            }).then(function (response) {
                // ensure the banners response is specified
                if (isDefinedAndNotNull(response.banners)) {
                    if (isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                        // update the header text
                        var bannerHeader = $('#banner-header').text(response.banners.headerText).show();

                        // show the banner
                        var updateTop = function (elementId) {
                            var element = $('#' + elementId);
                            element.css('top', (parseInt(bannerHeader.css('height')) + parseInt(element.css('top'))) + 'px');
                        };

                        // update the position of elements affected by top banners
                        updateTop('documentation-header');
                        updateTop('component-listing');
                        updateTop('component-usage-container');
                    }

                    if (isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                        // update the footer text and show it
                        var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();

                        var updateBottom = function (elementId) {
                            var element = $('#' + elementId);
                            element.css('bottom', parseInt(bannerFooter.css('height')) + parseInt(element.css('bottom')) + 'px');
                        };

                        // update the position of elements affected by bottom banners
                        updateBottom('component-filter-controls');
                        updateBottom('component-listing');
                        updateBottom('component-usage-container');
                    }
                }

                deferred.resolve();
            }, function () {
                deferred.reject();
            });
        } else {
            deferred.resolve();
        }
    }).promise();

    // get the about details
    var about = $.ajax({
        type: 'GET',
        url: '../nifi-api/controller/about',
        dataType: 'json'
    }).done(function (response) {
        var aboutDetails = response.about;

        // set the document title and the about title
        $('#nf-version').text(aboutDetails.version);
    });

    // once the banners have loaded, function with remainder of the page
    $.when(banners, about).always(function () {
        // define the function for filtering the list
        $('#component-filter').keyup(function () {
            applyFilter();
        }).focus(function () {
            if ($(this).hasClass('component-filter-list')) {
                $(this).removeClass('component-filter-list').val('');
            }
        }).blur(function () {
            if ($(this).val() === '') {
                $(this).addClass('component-filter-list').val('Filter');
            }
        }).addClass('component-filter-list').val('Filter');

        // get the component usage container to install the window listener
        var componentUsageContainer = $('#component-usage-container');

        // size the iframe accordingly
        var componentUsage = $('#component-usage').css({
            width: componentUsageContainer.width(),
            height: componentUsageContainer.height()
        });

        // add a window resize listener
        $(window).resize(function () {
            componentUsage.css({
                width: componentUsageContainer.width(),
                height: componentUsageContainer.height()
            });
        });

        // listen for loading of the iframe to update the title
        $('#component-usage').on('load', function () {
            var componentName = '';
            var href = $(this).contents().get(0).location.href;

            // see if the href ends in index.htm[l]
            var indexOfIndexHtml = href.indexOf('index.htm');
            if (indexOfIndexHtml >= 0) {
                href = href.substring(0, indexOfIndexHtml);
            }

            // remove the trailing separator
            if (href.length > 0) {
                var indexOfSeparator = href.lastIndexOf('/');
                if (indexOfSeparator === href.length - 1) {
                    href = href.substring(0, indexOfSeparator);
                }
            }

            // extract the simple name
            if (href.length > 0) {
                var indexOfLastDot = href.lastIndexOf('.');
                if (indexOfLastDot >= 0) {
                    var indexAfterStrToFind = indexOfLastDot + 1;
                    if (indexAfterStrToFind < href.length) {
                        componentName = href.substr(indexAfterStrToFind);
                    }
                }
            }

            // if we could figure out the name
            if (componentName !== '') {
                selectComponent(componentName);
            }
        });
        
        // listen for on the rest api and user guide and developer guide and admin guide and overview
        $('a.rest-api, a.user-guide, a.developer-guide, a.admin-guide, a.overview, a.expression-language-guide, a.getting-started').on('click', function() {
            selectComponent($(this).text());
        });

        // get the initial selection
        var initialComponentLink = $('a.component-link:first');
        var initialSelection = $('#initial-selection').text();
        if (initialSelection !== '') {
            $('a.component-link').each(function () {
                var componentLink = $(this);
                if (componentLink.text() === initialSelection) {
                    initialComponentLink = componentLink;
                    return false;
                }
            });
        }

        // click the first link
        initialComponentLink[0].click();
    });
});
