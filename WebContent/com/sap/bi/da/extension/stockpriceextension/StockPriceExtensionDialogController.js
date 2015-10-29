/*
Copyright 2015, SAP SE

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
       http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

define(function() {
    "use strict";

    var StockPriceExtensionDialogController = function(acquisitionState, oDeferred, fServiceCall, workflow, ExtensionUtils) {

    	/*
        Create dialog controls
        */
        var dLayout = new sap.ui.commons.layout.MatrixLayout({
            layoutFixed : true,
            columns : 2,
            width : "570px",
            widths : [ "20%", "80%" ]
        });
        
        // Dataset Name
        var datasetNameTxt = new sap.ui.commons.TextField({
            width : '100%',
            value : "",
            enabled : workflow === "CREATE"
        });

        var datasetNameLbl = new sap.ui.commons.Label({
            text : "Dataset Name:",
            labelFor : datasetNameTxt
        });

        dLayout.createRow({
            height : "30px"
        }, datasetNameLbl, datasetNameTxt);
        
        //Create a AutoComplete control and fill the items
        var stockData = ExtensionUtils.getStockSymbolsList();
        
        var stockSymbolTxt = new sap.ui.commons.AutoComplete({
        	tooltip: "Enter a stock symbol",
        	maxPopupItems: 5
        });
        
        for(var i=0; i<stockData.length; i++){
        	stockSymbolTxt.addItem(new sap.ui.core.ListItem({text: stockData[i].name}));
        }
        
        var stockSymbolLbl = new sap.ui.commons.Label({
            text : "Stock Symbol:",
            labelFor : stockSymbolTxt
        });

        dLayout.createRow({
            height : "30px"
        }, stockSymbolLbl, stockSymbolTxt);
        
        //start date
        var startDatePicker = new sap.ui.commons.DatePicker('date1');
        startDatePicker.setYyyymmdd(ExtensionUtils.calcLastNDaysFromToday(30));

        var startDateLbl = new sap.ui.commons.Label({
            text : "Start Date",
            labelFor : startDatePicker
        });

        dLayout.createRow({
            height : "30px"
        }, startDateLbl, startDatePicker);

        
        //End Date
        var endDatePicker = new sap.ui.commons.DatePicker('date2');
        endDatePicker.setYyyymmdd(ExtensionUtils.calcLastNDaysFromToday(0));

        var endDateLbl = new sap.ui.commons.Label({
            text : "End Date",
            labelFor : endDatePicker
        });

        dLayout.createRow({
            height : "30px"
        }, endDateLbl, endDatePicker);
        
        //API Key
        var APIKeyTxt = new sap.ui.commons.TextField({
            width : '100%',
            value : "xiXvzXD6mwdhUUjmECVc"
        });

        var APIKeyLbl = new sap.ui.commons.Label({
            text : "Quandl API Key:",
            labelFor : APIKeyTxt
        });

        dLayout.createRow({
            height : "30px"
        }, APIKeyLbl, APIKeyTxt);
                
        /*
        Button press events
        */
        var buttonCancelPressed = function() {
        	oDeferred.reject(); //promise fail
            dialog.close(); // dialog is hoisted from below
        };
        
        var setDocMetadata = ExtensionUtils.getMetadata();
        
        var buttonOKPressed = function() {
            var info = {};
            
            info.stocksymbol = stockSymbolTxt.getValue();
            info.startdate = startDatePicker.getValue();
            info.enddate = endDatePicker.getValue();
            info.apikey = APIKeyTxt.getValue();
            
            info.docmetadata = JSON.stringify(setDocMetadata);
            info.datasetName =  datasetNameTxt.getValue();
            
            acquisitionState.info = JSON.stringify(info);
            oDeferred.resolve(acquisitionState, datasetNameTxt.getValue());
            dialog.close();
        };

        var okButton = new sap.ui.commons.Button({
            press : [ buttonOKPressed, this ],
            text : "Send",
            tooltip : "Send"
        }).setStyle(sap.ui.commons.ButtonStyle.Accept);

        var cancelButton = new sap.ui.commons.Button({
            press : [ buttonCancelPressed, this ],
            text : "Cancel",
            tooltip : "Cancel"
        }).addStyleClass(sap.ui.commons.ButtonStyle.Default);

        var onClosed = function() {
            if (oDeferred.state() === "pending") {
                oDeferred.reject();
            }
        };
        
        /*
        Modify controls based on acquisitionState
        */
        var envProperties = acquisitionState.envProps;
        if (acquisitionState.info) {
            var info = JSON.parse(acquisitionState.info);

            stockSymbolTxt.setValue(info.stocksymbol);
            startDatePicker.setValue(info.startdate);
            endDatePicker.setValue(info.enddate);
            APIKeyTxt.setValue(info.apikey);
            
            envProperties.datasetName = info.datasetName;
        }
        datasetNameTxt.setValue(envProperties.datasetName);

        /*
        Create the dialog
        */
        var dialog = new sap.ui.commons.Dialog({
            width : "720px",
            height : "480px",
            modal : true,
            resizable : false,
            closed : function () {
                this.destroy();
                oDeferred.reject();
            },
            content: [dLayout],
            buttons : [okButton, cancelButton]
        });
        
        dialog.setTitle("StockPrice Extension: " + envProperties.datasetName);

        this.showDialog = function() {
            dialog.open();
        };
    };

    return StockPriceExtensionDialogController;
});