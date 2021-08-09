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
        var componentLinks = $(componentContainer).find('a.component-link, a.document-link');

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

    var selectComponent = function (selectedExtension, selectedBundleGroup, selectedBundleArtifact, selectedArtifactVersion) {
        var componentLinks = $('a.component-link');

        // consider each link
        $.each(componentLinks, function () {
            var componentLink = $(this);
            var item = componentLink.closest('li.component-item');
            var extension = item.find('span.extension-class').text();
            var group = item.find('span.bundle-group').text();
            var artifact = item.find('span.bundle-artifact').text();
            var version = item.find('span.bundle-version').text();

            if (extension === selectedExtension && group === selectedBundleGroup
                && artifact === selectedBundleArtifact && version === selectedArtifactVersion) {

                // remove all selected styles
                $('li.component-item').removeClass('selected');

                // select this links item
                item.addClass('selected');

                // set the header
                $('#selected-component').text(componentLink.text());

                // stop iteration
                return false;
            }
        });
    };

    var selectDocument = function (documentName) {
        var documentLinks = $('a.document-link');

        // consider each link
        $.each(documentLinks, function () {
            var documentLink = $(this);
            if (documentName === $.trim(documentLink.text())) {
                // remove all selected styles
                $('li.component-item').removeClass('selected');

                // select this links item
                documentLink.closest('li.component-item').addClass('selected');

                // set the header
                $('#selected-component').text(documentLink.text());

                // stop iteration
                return false;
            }
        });
    };

    // get the banners if we're not in the shell
    var bannerHeaderHeight = 0;
    var bannerFooterHeight = 0;
    var banners = $.Deferred(function (deferred) {
        if (top === window) {
            $.ajax({
                type: 'GET',
                url: '../nifi-api/flow/banners',
                dataType: 'json'
            }).then(function (response) {
                // ensure the banners response is specified
                if (isDefinedAndNotNull(response.banners)) {
                    if (isDefinedAndNotNull(response.banners.headerText) && response.banners.headerText !== '') {
                        // update the header text
                        var bannerHeader = $('#banner-header').text(response.banners.headerText).show();
                        bannerHeaderHeight = bannerHeader.height();
                    }

                    if (isDefinedAndNotNull(response.banners.footerText) && response.banners.footerText !== '') {
                        // update the footer text and show it
                        var bannerFooter = $('#banner-footer').text(response.banners.footerText).show();
                        bannerFooterHeight = bannerFooter.height();
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
        url: '../nifi-api/flow/about',
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

        // get the component containers to install the window listener
        var documentationHeader = $('#documentation-header');
        var componentRootContainer = $('#component-root-container');
        var componentListingContainer = $('#component-listing-container', componentRootContainer);
        var componentListing = $('#component-listing', componentListingContainer);
        var componentFilterControls = $('#component-filter-controls', componentRootContainer);
        var componentUsageContainer = $('#component-usage-container', componentUsageContainer);
        var componentUsage = $('#component-usage', componentUsageContainer);

        var componentListingContainerPaddingX = 0;
        componentListingContainerPaddingX += parseInt(componentListingContainer.css("padding-right"), 10);
        componentListingContainerPaddingX += parseInt(componentListingContainer.css("padding-left"), 10);

        var componentListingContainerPaddingY = 0;
        componentListingContainerPaddingY += parseInt(componentListingContainer.css("padding-top"), 10);
        componentListingContainerPaddingY += parseInt(componentListingContainer.css("padding-bottom"), 10);

        var componentUsageContainerPaddingX = 0;
        componentUsageContainerPaddingX += parseInt(componentUsageContainer.css("padding-right"), 10);
        componentUsageContainerPaddingX += parseInt(componentUsageContainer.css("padding-left"), 10);

        var componentUsageContainerPaddingY = 0;
        componentUsageContainerPaddingY += parseInt(componentUsageContainer.css("padding-top"), 10);
        componentUsageContainerPaddingY += parseInt(componentUsageContainer.css("padding-bottom"), 10);

        var componentListingContainerMinWidth = parseInt(componentListingContainer.css("min-width"), 10) + componentListingContainerPaddingX;
        var componentUsageContainerMinWidth = parseInt(componentUsageContainer.css("min-width"), 10) + componentUsageContainerPaddingX;
        var smallDisplayBoundary = componentListingContainerMinWidth + componentUsageContainerMinWidth;

        var cssComponentListingNormal = { backgroundColor: "#ffffff" };
        var cssComponentListingSmall = { backgroundColor: "#fbfbfb" };

        // add a window resize listener
        $(window).resize(function () {
            // This -1 is the border-top of #component-usage-container
            var baseHeight = window.innerHeight - 1;
            baseHeight -= bannerHeaderHeight;
            baseHeight -= bannerFooterHeight;
            baseHeight -= documentationHeader.height();

            // resize component list accordingly
            if (smallDisplayBoundary > window.innerWidth) {
                // screen is not wide enough to display content usage
                // within the same row.
                componentListingContainer.css(cssComponentListingSmall);
                componentListingContainer.css({
                    borderBottom: "1px solid #ddddd8"
                });
                componentListing.css({
                    height: "200px"
                });
                // resize the iframe accordingly
                var componentUsageHeight = baseHeight;
                if (componentListingContainer.is(":visible")) {
                    componentUsageHeight -= componentListingContainer.height();
                    componentUsageHeight -= 1; // border-bottom
                }
                componentUsageHeight -= componentListingContainerPaddingY;
                componentUsageHeight -= componentUsageContainerPaddingY;
                componentUsage.css({
                    width: componentUsageContainer.width(),
                    height: componentUsageHeight
                });
                componentUsageContainer.css({
                    height: componentUsage.height()
                });
            } else {
                componentListingContainer.css(cssComponentListingNormal);

                var componentListingHeight = baseHeight;
                componentListingHeight -= componentFilterControls.height();
                componentListingHeight -= componentListingContainerPaddingY;
                componentListing.css({
                    height: componentListingHeight
                });

                // resize the iframe accordingly
                componentUsage.css({
                    width: componentUsageContainer.width(),
                    height: baseHeight - componentUsageContainerPaddingY
                });
                componentUsageContainer.css({
                    height: componentUsage.height()
                });
                componentListingContainer.css({
                    borderBottom: "0px"
                });
            }
        });


        var toggleComponentListing = $('#component-list-toggle-link');
        toggleComponentListing.click(function(){
            componentListingContainer.toggle(0, function(){
                toggleComponentListing.text($(this).is(":visible") ? "-" : "+");
                $(window).resize();
            });
        });

        // listen for loading of the iframe to update the title
        $('#component-usage').on('load', function () {

            // resize window accordingly.
            $(window).resize();

            var bundleAndComponent = '';
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

            // remove the beginning bits
            if (href.length > 0) {
                var path = 'nifi-docs/components';
                var indexOfPath = href.indexOf(path);
                if (indexOfPath >= 0) {
                    var indexOfBundle = indexOfPath + path.length + 1;
                    if (indexOfBundle < href.length) {
                        bundleAndComponent = href.substr(indexOfBundle);
                    }
                }
            }

            // if we could extract the bundle coordinates
            if (bundleAndComponent !== '') {
                var bundleTokens = bundleAndComponent.split('/');
                if (bundleTokens.length === 4) {
                    selectComponent(bundleTokens[3], bundleTokens[0], bundleTokens[1], bundleTokens[2]);
                }
            }
        });
        
        // listen for on the rest api and user guide and developer guide and admin guide and overview
        $('a.document-link').on('click', function() {
            selectDocument($(this).text());
        });

        // get the initial selection
        var initialLink = $('a.document-link:first');
        var initialSelectionType = $.trim($('#initial-selection-type').text());

        if (initialSelectionType !== '') {
            var initialSelectionBundleGroup = $.trim($('#initial-selection-bundle-group').text());
            var initialSelectionBundleArtifact = $.trim($('#initial-selection-bundle-artifact').text());
            var initialSelectionBundleVersion = $.trim($('#initial-selection-bundle-version').text());

            $('a.component-link').each(function () {
                var componentLink = $(this);
                var item = componentLink.closest('li.component-item');
                var extension = item.find('span.extension-class').text();
                var group = item.find('span.bundle-group').text();
                var artifact = item.find('span.bundle-artifact').text();
                var version = item.find('span.bundle-version').text();

                if (extension === initialSelectionType && group === initialSelectionBundleGroup
                        && artifact === initialSelectionBundleArtifact && version === initialSelectionBundleVersion) {
                    initialLink = componentLink;
                    return false;
                }
            });
        }

        // click the first link
        initialLink[0].click();
    });
});
