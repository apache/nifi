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
$(document).ready(function () {

    // NOTE: rule is synonymous with threshold

    //local variable declarations
    var queryParamsObj = getQueryParameters(this.URL);

    var baseuri = 'resources/settings/processor/' + queryParamsObj.processorId + '/',
            //grid references
            attribute_grid,
            //state variables
            selected_attribute = 0,
            selected_threshold_id = 0,
            selected_threshold_grid;

    $('#attribute-filter-values').hide();

    addHoverEffect('div.button', 'button-normal', 'button-over');
    $('body').click(function (e) {
        //hideContextMenu();
        hideAttributeContextMenu();
        hideThresholdContextMenu();
    });

    //confirmation dialog creation.
    var error_dialog = $('#error-dialog').modal({
        headerText: '',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Ok',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        error_dialog.modal("hide");
                    },
                    error: function (e, obj) {
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {

            }
        }
    });
    $('.dialog-border')[0].style.zIndex = '1302';

    /***************************************************************************************************************************************/
    //  Attribute methods and functions
    //  
    //  Handles Select, Add, Update, Delete, and error messages related to Attribute Management.  See below for Threshold mgmt.
    //
    //
    /****************************************************************************************************************************************/
    attribute_grid = $('#list').jqGrid({
        url: baseuri + 'attributes' + '?' + $.param(queryParamsObj),
        autoheight: true,
        autowidth: true,
        colNames: ['UUId', 'Names'],
        colModel: [{name: 'uuid', index: 'uuid', hidden: true},
            {name: 'attributename', index: 'attributename'}
        ],
        rowNum: -1,
        subGrid: true,
        sortname: 'attributename',
        sortorder: 'asc',
        viewrecords: true,
        postData: {attributefilter: $('#attributefilter').val()},
        subGridRowExpanded: function (subgrid_id, row_id) {
            hideAttributeContextMenu();
            expandDetail(subgrid_id, row_id);
            addRowHighlight(row_id);
        },
        subGridRowColapsed: function (subgrid_id, row_id) {
            hideThresholdContextMenu();
            removeRowHighlight(row_id);
        },
        gridComplete: function () {
            var ids = attribute_grid.jqGrid('getDataIDs');
            for (var i = 0; i < ids.length; i++)
            {
                var id = ids[i];
                setAttributeRowDefault(id);
                setRowBorder(id);
            }
            setAttributeGridStatus();
            $('.ui-jqgrid-hdiv').hide();
            setMainGridSize();
        },
        xmlReader: {
            root: "attributes",
            row: "attribute",
            repeatitems: false,
            id: "uuid"
        }
    });


    var attribute_add_dialog = $('#attribute-add-dialog').modal({
        headerText: 'Add Attribute',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        $.ajax({
                            type: "PUT",
                            url: baseuri + 'attribute' + '?' + $.param(queryParamsObj),
                            data: {attributename: $('#new-attribute-name').val(),
                                size: $('#size-value').val(),
                                count: $('#file-count-value').val()},
                            success: function (msg) {
                                attribute_grid.trigger("reloadGrid");
                                attribute_add_dialog.modal('hide');
                                setMainGridSize();

                            },
                            error: function (e, obj) {
                                setErrorMessage(e.responseText);
                            }
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        attribute_add_dialog.modal('hide');
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#new-attribute-name').val("");
                $('#size-value').val("");
                $('#file-count-value').val("");

            }
        }
    });

    var attribute_edit_dialog = $('#attribute-edit-dialog').modal({
        headerText: 'Edit Attribute',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        $.ajax({
                            type: "POST",
                            url: baseuri + 'attribute' + '?' + $.param(queryParamsObj),
                            data: {procid: queryParamsObj.processorId,
                                attributeuuid: selected_attribute,
                                attributename: $('#edit-attribute-name').val()
                            },
                            success: function (msg) {
                                attribute_grid.trigger("reloadGrid");
                                attribute_edit_dialog.modal('hide');
                            },
                            error: function (e, obj) {
                                setErrorMessage(e.responseText);
                            }
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        attribute_edit_dialog.modal('hide');
                        hideAttributeContextMenu();
                        selected_attribute = 0;
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#edit-attribute-name').val("");
                hideAttributeContextMenu();

            }
        }
    });

    var attribute_filter_dialog = $('#attribute-filter-dialog').modal({
        headerText: 'Filter Attributes',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        var text = " <img alt=\"Separator\" src=\"images/separator.gif\">" +
                                " <input id=\"attribute-clear-filter\"  class=\"control-style\" type=\"image\" src=\"images/clear.png\" title = \"Clear Attribute Filters\" />" +
                                "<div class=\"clear-filter-label\" >Current Filter </b> - Attribute Name: </div><div class=\"clear-filter-text\" >" + $('#filter-attribute').val() + "</div>";
                        attribute_grid.jqGrid('setGridParam', {postData: {attributefilter: $('#filter-attribute').val(), rownum: attribute_grid.getGridParam('rowNum')}});
                        attribute_grid.trigger('reloadGrid');
                        attribute_filter_dialog.modal('hide');
                        $('#attribute-filter-values').html(text);
                        $('#attribute-filter-values').show();
                        $('#attribute-clear-filter').click(function () {
                            clearAttributeFilter()
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        attribute_filter_dialog.modal('hide');
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#filter-attribute').val("");
            }
        }
    });

    var delete_attribute_confirm = $('#attribute-confirm-dialog').modal({
        headerText: '',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Ok',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        deleteAttributeRow(selected_attribute);
                        // attribute_grid.trigger("reloadGrid");
                    },
                    error: function (e, obj) {
                    }
                }}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        delete_attribute_confirm.modal('hide');
                        hideAttributeContextMenu();
                        selected_attribute = 0;
                        attribute_grid.trigger("reloadGrid");
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                hideAttributeContextMenu();
            }
        }
    });

    $('#attribute-add').click(function () {
        addAttribute();
    });

    $('#attribute-add-main').click(function () {
        addAttribute();
    });

    $('#attribute-edit').click(function () {
        // open the edit property dialog
        attribute_edit_dialog.modal('show');
        $('#edit-attribute-name').focus();
        var attribute = attribute_grid.jqGrid('getRowData', selected_attribute).attributename;
        $('#edit-attribute-name').val(attribute);
    });

    $('#attribute-delete').click(function (e) {
        delete_attribute_confirm.modal('show');
        $('#attribute-dialog-content').html("");
        var attrname = attribute_grid.jqGrid('getRowData', selected_attribute).attributename;
        $('#attribute-dialog-content').html("Are you sure you want to delete the " + attrname + " attribute and all of its associated thresholds?");
    })

    $('#attribute-filter').click(function () {
        attributeFilter();
    })

    $("#attribute-names-header").click(function (e) {
        attribute_grid.jqGrid("sortGrid", "attributename", false);
    })

    function addAttribute() {
        attribute_add_dialog.modal('show');
        $('#new-attribute-name').focus();
    }

    function setAttributeRowDefault(id) {
        attribute_grid.restoreRow(id);
        $('#' + id).bind("contextmenu", function (e) {
            hideAttributeContextMenu();
            selected_attribute = id;
            addRowHighlight(selected_attribute);
            attributeRightClick(e);
            return false;
        });
        $('#' + id).removeClass("jqgrow");
    }

    function attributeRightClick(e) {
        $('#attribute-context-menu').show();
        $('#attribute-context-menu').css("left", e.clientX + 10 + "px");
        $('#attribute-context-menu').css("display", "inline-block");
        $('#attribute-context-menu').css("top", e.clientY + 10 + "px");
        $('#threshold-context-menu').hide();
        hideThresholdContextMenu();
    }

    function deleteAttributeRow(attribute_id) {
        $.ajax({
            type: "DELETE",
            url: baseuri + 'attribute' + '?' + $.param(queryParamsObj),
            data: {id: attribute_id},
            success: function (msg) {
                delete_attribute_confirm.modal('hide');
                attribute_grid.trigger("reloadGrid");
                selected_attribute = 0;
            },
            error: function (e, obj) {
                delete_attribute_confirm.modal('hide');
                setErrorMessage(obj.responseText);
            }
        });
    }


    function attributeFilter() {
        attribute_filter_dialog.modal('show');
    }

    function clearAttributeFilter() {
        $('#attributefilter').val("");
        $('#attribute-filter-values').html("");
        $('#attribute-filter-values').hide();
        attribute_grid.jqGrid('setGridParam', {postData: {attributefilter: "", rownum: attribute_grid.getGridParam('rowNum')}});
        attribute_grid.trigger('reloadGrid');
    }

    function setAttributeGridStatus() {
        $.ajax({
            type: "GET",
            url: baseuri + 'attributeInfo' + '?' + $.param(queryParamsObj),
            //data:{rownum:attribute_grid.getGridParam('rowNum')},
            success: function (msg) {
                $('#status-bar').html("");
                $('#status-bar').html(msg);
            },
            error: function (e, obj) {
                setErrorMessage(e.responseText);
            }
        });
    }


    /***************************************************************************************************************************************/
    //  Threshold Grid methods and functions
    //  
    //
    /****************************************************************************************************************************************/
    function expandDetail(subgrid_id, attribute_id) {
        var subgrid_table_id = subgrid_id + "_t",
                subdiv = $("#" + subgrid_id);
        subdiv.html(
                "<div id=\"threshold-" + attribute_id + "-filter-values\" class=\"clear-filter\"/>" +
                "<table>" +
                "<tr>" +
                "<td><div id=\"" + attribute_id + "_threshold-value-header\" class=\"thresholds-abbreviated-header\"  >Attribute Value</div></td>" +
                "<td><div id=\"" + attribute_id + "_threshold-size-header\" class=\"thresholds-abbreviated-header\"  >Size (bytes)</div></td>" +
                "<td><div id=\"" + attribute_id + "_threshold-count-header\" class=\"thresholds-abbreviated-header\" >File Count</div></td>" +
                "</tr>" +
                "</table>" +
                "<table id='" + subgrid_table_id + "'>" +
                "</table>");
        var subgrid = $('#' + subgrid_table_id);

        subgrid.jqGrid({
            autoheight: true,
            autowidth: true,
            url: baseuri + 'attribute/' + attribute_id + '/rules' + '?' + $.param(queryParamsObj),
            datatype: "xml",
            colNames: ['', 'Attribute Value', 'Size (bytes)', 'File Count'],
            colModel: [{name: 'ruuid', index: 'ruuid', width: 175, hidden: true},
                {name: 'attributevalue', index: 'attributevalue', width: 175}, //, editable:true},
                {name: 'size', index: 'size', width: 100}, //, editable:true},
                {name: 'count', index: 'count', width: 125}//, editable:true}

            ],
            rowNum: -1,
            sortname: 'attributevalue',
            sortorder: 'asc',
            gridComplete: function () {
                $('.ui-jqgrid-hdiv').hide();
                var ids = subgrid.jqGrid('getDataIDs');
                var height = 20;
                for (var i = 0; i < ids.length; i++)
                {
                    height = height + 23;
                    var id = ids[i];
                    setThresholdRowDefault(subgrid, id);
                    setRowBorder(id);
                }
                $('#list_' + attribute_id).width($('#list_' + attribute_id).width() - 30);
                subgrid.setGridHeight(height);
                setTimeout(function () {
                    $('.tablediv').css("position", "relative");
                    $('.tablediv').css("left", "30px");
                    $('.ui-icon-carat-1-sw').css("position", "relative");
                    $('.ui-icon-carat-1-sw').css("left", "20px");
                    $("#" + attribute_id + "_threshold-value-header").width(subgrid[0].grid.cols[1].clientWidth);
                    $("#" + attribute_id + "_threshold-size-header").width(subgrid[0].grid.cols[2].clientWidth - 2);
                    $("#" + attribute_id + "_threshold-count-header").width(subgrid[0].grid.cols[3].clientWidth - 6);
                }, 5);

            },
            postData: {thresholdfilter: $('#attributevaluefilter').val(), sizefilter: $('#sizefilter').val(), filecountfilter: $('#filecountfilter').val()},
            xmlReader: {
                root: "rules",
                row: "rule",
                repeatitems: false,
                id: "ruuid"}
        });

        hideThresholdContextMenu();

        $("#" + attribute_id + "_threshold-value-header").click(function (e) {
            subgrid.jqGrid("sortGrid", "attributevalue", false);
        });
        $("#" + attribute_id + "_threshold-size-header").click(function (e) {
            subgrid.jqGrid("sortGrid", "size", false);
        });
        $("#" + attribute_id + "_threshold-count-header").click(function (e) {
            subgrid.jqGrid("sortGrid", "count", false);
        });
        subgrid.setGridWidth(subgrid[0].grid.width - 60);

    }

    //threshold add dialog creation
    var threshold_add_dialog = $('#threshold-add-dialog').modal({
        headerText: 'Add New Thresholds',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        $.ajax({
                            type: "PUT",
                            url: baseuri + 'attribute/' + selected_attribute + '/rule' + '?' + $.param(queryParamsObj),
                            data: {attributevalue: $('#add-attribute-value').val(), size: $('#add-size-value').val(), count: $('#add-file-count-value').val()},
                            success: function (msg) {
                                // threshold_grid.trigger("reloadGrid");
                                selected_threshold_grid.trigger("reloadGrid");
                                threshold_add_dialog.modal('hide');
                            },
                            error: function (e, obj) {
                                setErrorMessage(e.responseText);
                            }
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        threshold_add_dialog.modal('hide');
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#add-attribute-value').val("");
                $('#add-size-value').val("");
                $('#add-file-count-value').val("");
                // threshold_dialog.modal('show');
            }
        }
    });

    //threshold add dialog creation
    var threshold_edit_dialog = $('#threshold-edit-dialog').modal({
        headerText: 'Edit Thresholds',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        $.ajax({
                            type: "POST",
                            url: baseuri + 'attribute/' + selected_attribute + '/rule/' + $('#edit-attribute-value').val() + '?' + $.param(queryParamsObj),
                            data: {size: $('#edit-size-value').val(), count: $('#edit-file-count-value').val()},
                            success: function (msg) {
                                // threshold_grid.trigger("reloadGrid");
                                selected_threshold_grid.trigger("reloadGrid");
                                threshold_edit_dialog.modal('hide');
                            },
                            error: function (e, obj) {
                                setErrorMessage(e.responseText);
                            }
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        threshold_edit_dialog.modal('hide');

                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#edit-attribute-value').val("");
                $('#edit-size-value').val("");
                $('#edit-file-count-value').val("");
                hideThresholdContextMenu();
            }
        }
    });

    var threshold_filter_dialog = $('#threshold-filter-dialog').modal({
        headerText: 'Filter Thresholds',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Apply',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        var text = "<input id=\"threshold-clear-filter\"" +
                                "class=\"control-style\" type=\"image\" src=\"images/clear.png\" title = \"Clear Values-Thresholds Filters\" />" +
                                "<div class=\"clear-filter-label\" >Current Filter </b> - Attribute Value: </div><div class=\"clear-filter-text\" >" +
                                $('#filter-attribute-value').val() + "</div><div class=\"clear-filter-label\" > Size: </div><div class=\"clear-filter-text\" >" +
                                $('#filter-size').val() + "</div><div class=\"clear-filter-label\" > Count: </div><div class=\"clear-filter-text\" >" +
                                $('#filter-count').val() + "</div>";
                        selected_threshold_grid.jqGrid('setGridParam', {postData: {attributevaluefilter: $('#filter-attribute-value').val(), sizefilter: $('#filter-size').val(), filecountfilter: $('#filter-count').val(), rownum: selected_threshold_grid.getGridParam('rowNum')}});
                        selected_threshold_grid.trigger('reloadGrid');
                        threshold_filter_dialog.modal('hide');

                        $('#threshold-' + selected_attribute + '-filter-values').html(text);
                        $('#threshold-' + selected_attribute + '-filter-values').show();
                        $('#threshold-clear-filter').click(function (e) {
                            clearThresholdFilter();
                        });
                    }}}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        threshold_filter_dialog.modal('hide');
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                // reset dialog (ie clear content, etc)
                $('#filter-attribute-value').val("");
                $('#filter-size').val("");
                $('#filter-count').val("");

            }
        }
    });

    var delete_threshold_confirm = $('#threshold-confirm-dialog').modal({
        headerText: '',
        overlayBackground: false,
        buttons: [{
                buttonText: 'Ok',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        if (selected_threshold_id != "-1")
                            deleteThresholdRow(selected_threshold_id);
                        else {
                            delete_threshold_confirm.modal('hide');
                            setErrorMessage("The 'Default' threshold cannot be deleted");
                        }
                    },
                    error: function (e, obj) {
                    }
                }}
            , {
                buttonText: 'Cancel',
                handler: {
                    scope: this,
                    args: [],
                    click: function () {
                        // close the dialog
                        delete_threshold_confirm.modal('hide');
                    }
                }
            }],
        handler: {
            scope: this,
            args: [],
            close: function () {
                hideThresholdContextMenu();
            }
        }
    });

    function setThresholdRowDefault(grid, id) {
        $('#' + id).bind("contextmenu", function (e) {
            $('#attribute-context-menu').hide();
            selected_threshold_grid = grid;
            removeRowHighlight(selected_threshold_id);
            selected_threshold_id = id;
            var attributeid = this.offsetParent.id.replace('list_', '');
            attributeid = attributeid.replace('_t', '');
            selected_attribute = attributeid;
            $('#threshold-context-menu').show();
            $('#threshold-context-menu').css("left", e.clientX + 10 + "px");
            $('#threshold-context-menu').css("display", "inline-block");
            $('#threshold-context-menu').css("top", e.clientY + 10 + "px");
            var thresholdname = selected_threshold_grid.jqGrid('getRowData', selected_threshold_id).attributevalue;
            addRowHighlight(selected_threshold_id);
            if (thresholdname == "Default")
                $('#threshold-delete').hide();
            else
                $('#threshold-delete').show();
            hideAttributeContextMenu();
            return false;
        })
        $('#' + id).removeClass("jqgrow");
        setDefaultStyle(grid, id);
    }

    function setDefaultStyle(grid, id)
    {
        var thresholdname = grid.jqGrid('getRowData', id).attributevalue;
        if (thresholdname == "Default")
            $('#' + id).css('background', 'none repeat scroll 0 0 #DDECFE');
    }

    function deleteThresholdRow(threshold_id) {
        var attribute_id = selected_attribute;
        var thresholdId = selected_threshold_grid.jqGrid('getRowData', threshold_id).attributevalue;
        if (thresholdId == "Default") {
            delete_threshold_confirm.modal('hide');
            setErrorMessage("The \"Default\" threshold cannot be deleted.");
        }
        else {
            $.ajax({
                type: "DELETE",
                url: baseuri + 'attribute/' + attribute_id + '/rule/' + thresholdId + '?' + $.param(queryParamsObj),
                success: function (msg) {
                    delete_threshold_confirm.modal('hide');
                    selected_threshold_grid.trigger("reloadGrid");
                },
                error: function (e, obj) {
                    delete_threshold_confirm.modal('hide');
                    setErrorMessage(obj.responseText);
                }
            });
        }
    }

    $('#threshold-filter').click(function () {
        thresholdFilter();
    });

    //Add threshold icon click handler
    $('#threshold-add').click(function () {
        // open the new property dialog
        threshold_add_dialog.modal('show');
        $('#add-attribute-value').focus();

    });

    $('#threshold-edit').click(function () {
        // open the edit property dialog
        threshold_edit_dialog.modal('show');

        $('#edit-threshold-name').focus();
        var attributeValue = selected_threshold_grid.jqGrid('getRowData', selected_threshold_id).attributevalue;
        var size = selected_threshold_grid.jqGrid('getRowData', selected_threshold_id).size;
        var count = selected_threshold_grid.jqGrid('getRowData', selected_threshold_id).count;

        $('#edit-attribute-value').val(attributeValue);
        $('#edit-size-value').val(size);
        $('#edit-file-count-value').val(count);
    });

    $('#threshold-delete').click(function (e) {
        delete_threshold_confirm.modal('show');
        var thresholdname = selected_threshold_grid.jqGrid('getRowData', selected_threshold_id).attributevalue;
        $('#threshold-dialog-content').html("");
        $('#threshold-dialog-content').html("Are you sure you want to delete the threshold for value " + thresholdname + "?");

    });

    $('#threshold-filter').click(function () {
        thresholdFilter();
    });

    $('#threshold-clear-filter').click(function () {
        clearThresholdFilter();
    });

    function thresholdFilter() {
        threshold_filter_dialog.modal('show');
    }

    function clearThresholdFilter(e) {
        $('#threshold-' + selected_attribute + '-filter-values').hide();
        $('#attributevaluefilter').val("");
        $('#sizefilter').val("");
        $('#filecountfilter').val("");
        selected_threshold_grid.jqGrid('setGridParam', {postData: {attributevaluefilter: "", sizefilter: "", filecountfilter: ""}});
        selected_threshold_grid.trigger('reloadGrid');
    }

    /***************************************************************************************************************************************/
    //  General Helper Functions
    //  
    //  
    //
    /****************************************************************************************************************************************/

    function getQueryParameters(url) {
        var result = {};

        var queryString = url.split("?")[1];
        var queryParams = queryString.split("&");
        for (var i = 0; i < queryParams.length; i++) {
            var pair = queryParams[i].split("=");
            result[pair[0]] = pair[1];
        }
        return result;
    }

    function hideContextMenu() {
        $('#attribute-context-menu').hide();
        $('#threshold-context-menu').hide();
    }

    function hideAttributeContextMenu()
    {
        $('#attribute-context-menu').hide();
        if (selected_attribute.length > 0) {
            if ($('#' + selected_attribute)[0].firstChild.className.indexOf("sgexpanded") == -1 &&
                    delete_attribute_confirm.css('display') == 'none' &&
                    attribute_edit_dialog.css('display') == 'none') {
                removeRowHighlight(selected_attribute);
                selected_attribute = 0;
            }
        }
    }

    function hideThresholdContextMenu()
    {
        $('#threshold-context-menu').hide();

        if (selected_threshold_id.length > 0 && delete_threshold_confirm.css('display') == 'none' && threshold_edit_dialog.css('display') == 'none') {
            removeRowHighlight(selected_threshold_id);
            setDefaultStyle(selected_threshold_grid, selected_threshold_id);
            selected_threshold_id = 0;
        }
    }

    function setRowBorder(selector) {
        $('#' + selector).children().css("borderBottom", "solid 2px #EEEEEE");
    }

    function removeRowHighlight(selector)
    {
        $('#' + selector).css("background", "#FFFFFF");
    }

    function addRowHighlight(selector)
    {
        $('#' + selector).css("background", "#FFE8B3");
    }

    //sets the message for an error message.
    function setErrorMessage(message) {
        $('#error-dialog-content').html("");
        $('#error-dialog-content').html(message);
        error_dialog.modal("show");
    }

    function toggleRowEdit(grid, ids, id) {
        if (ids.indexOf(id) > -1)
            grid.jqGrid('editRow', id);
        else
            grid.restoreRow(id);
    }

    setMainGridSize();
    $(window).resize(function () {
        setMainGridSize();

    })

    //sets the size of the attribute grid when a main window resize event occurs.
    function setMainGridSize() {
        attribute_grid.setGridWidth($('#list1').width());
        var body_height = this.frameElement.clientHeight;
        var other_height = $('#threshold-header-text').height() + $('.list-header')[0].clientHeight;
        var attribute_height = body_height - other_height;
        attribute_grid.setGridHeight(attribute_height - 40);
        setTimeout(function () {
            var attribute_width = attribute_grid[0].grid.width;
            if (attribute_grid[0].grid.cols.length == 0 || attribute_grid[0].grid.cols[2].clientWidth == 0) {
                $("#attribute-names-header").width(attribute_width);
            }
            else {
                $("#attribute-names-header").width(attribute_grid[0].grid.cols[2].clientWidth);
            }
        }, 10);
    }

    function addHoverEffect(selector, normalStyle, overStyle) {
        $(document).on('mouseenter', selector, function () {
            $(this).removeClass(normalStyle).addClass(overStyle);
        }).on('mouseleave', selector, function () {
            $(this).removeClass(overStyle).addClass(normalStyle);
        });
        return $(selector).addClass(normalStyle);
    }
});