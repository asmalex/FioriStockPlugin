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

package com.sap.bi.da.extension.stockpriceextension;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

import com.sap.bi.da.extension.sdk.DAEWorkflow;
import com.sap.bi.da.extension.sdk.DAException;
import com.sap.bi.da.extension.sdk.IDAEAcquisitionJobContext;
import com.sap.bi.da.extension.sdk.IDAEAcquisitionState;
import com.sap.bi.da.extension.sdk.IDAEClientRequestJob;
import com.sap.bi.da.extension.sdk.IDAEDataAcquisitionJob;
import com.sap.bi.da.extension.sdk.IDAEEnvironment;
import com.sap.bi.da.extension.sdk.IDAEMetadataAcquisitionJob;
import com.sap.bi.da.extension.sdk.IDAEProgress;
import com.sap.bi.da.extension.sdk.IDAExtension;
import com.turbomanage.httpclient.BasicHttpClient;
import com.turbomanage.httpclient.HttpResponse;
import com.turbomanage.httpclient.ParameterMap;
import com.univocity.parsers.common.processor.RowListProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

public class StockPriceExtension implements IDAExtension {
	
	static public final String EXTENSION_ID = "com.sap.bi.da.extension.stockpriceextension";
	private IDAEEnvironment environment;

    public StockPriceExtension() {
    }

    @Override
    public void initialize(IDAEEnvironment environment) {
    	this.environment = environment;
    	// This function will be called when the extension is initially loaded
    	// This gives the extension to perform initialization steps, according to the provided environment
    }

    @Override
    public IDAEAcquisitionJobContext getDataAcquisitionJobContext (IDAEAcquisitionState acquisitionState) {
        return new StockPriceExtensionAcquisitionJobContext(environment, acquisitionState);
    }

    @Override
    public IDAEClientRequestJob getClientRequestJob(String request) {
        return new StockPriceExtensionClientRequestJob(request);
    }

    private static class StockPriceExtensionAcquisitionJobContext implements IDAEAcquisitionJobContext {

        private IDAEAcquisitionState acquisitionState;
        private IDAEEnvironment environment;

        StockPriceExtensionAcquisitionJobContext(IDAEEnvironment environment, IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
            this.environment = environment;
        }

        @Override
        public IDAEMetadataAcquisitionJob getMetadataAcquisitionJob() {
            return new StockPriceExtensionMetadataRequestJob(acquisitionState);
        }

        @Override
        public IDAEDataAcquisitionJob getDataAcquisitionJob() {
            return new StockPriceExtensionDataRequestJob(environment, acquisitionState);
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        	// Provides the job the opportunity to perform cleanup, if needed
        	// Will be called after both job.cleanup()'s are called
        }
    }
    
    public static Reader newReader(InputStream input) {
		return newReader(input, (Charset) null);
	}

	public static Reader newReader(InputStream input, String encoding) {
		return newReader(input, Charset.forName(encoding));
	}

	public static Reader newReader(InputStream input, Charset encoding) {
		if (encoding != null) {
			return new InputStreamReader(input, encoding);
		} else {
			return new InputStreamReader(input);
		}
	}
    
    public static Reader newReader(File file) {
		return newReader(file, (Charset) null);
	}

	public static Reader newReader(File file, String encoding) {
		return newReader(file, Charset.forName(encoding));
	}

	public static Reader newReader(File file, Charset encoding) {
		FileInputStream input;
		try {
			input = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new IllegalArgumentException(e);
		}

		return newReader(input, encoding);
	}
    
    private static class StockPriceExtensionDataRequestJob implements IDAEDataAcquisitionJob
    {
        IDAEAcquisitionState acquisitionState;
        IDAEEnvironment environment;

        StockPriceExtensionDataRequestJob (IDAEEnvironment environment, IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
            this.environment = environment;
        }

        @Override
        public File execute(IDAEProgress callback) throws DAException {
        	
            try {
                JSONObject infoJSON = new JSONObject(acquisitionState.getInfo());
                JSONArray responseJSON;
                
              //read all info parameters
                String StockSymbol = infoJSON.getString("stocksymbol"); 
                String StartDate = infoJSON.getString("startdate");
                String EndDate = infoJSON.getString("enddate");
                String APIKey = infoJSON.getString("apikey");
                String charset = "UTF-8";
                String Url = "https://www.quandl.com";
                
                BasicHttpClient client = new BasicHttpClient(Url);
                ParameterMap params = client.newParams()
                        .add("auth_token", APIKey)
                        .add("trim_start", StartDate)
                        .add("trim_end", EndDate);
//                client.addHeader("name", "value");
                client.setConnectionTimeout(2000);
                
                CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
                
                HttpResponse response = client.get("/api/v1/datasets/WIKI/"+StockSymbol+".csv", params);
                
                InputStream stream = new ByteArrayInputStream(response.getBodyAsString().getBytes(StandardCharsets.UTF_8));
                
                CsvParserSettings settings = new CsvParserSettings();
                RowListProcessor rowProcessor = new RowListProcessor();
                settings.setRowProcessor(rowProcessor);
                settings.setLineSeparatorDetectionEnabled(true);
                settings.setHeaderExtractionEnabled(true);
                settings.selectFields("Date", "Adj. Open", "Adj. High", "Adj. Low", "Adj. Close", "Adj. Volume");
                
                CsvParser parser = new CsvParser(settings);
                
                parser.parse(newReader(stream));
                String[] headers = rowProcessor.getHeaders();
                List<String[]> rows = rowProcessor.getRows();
                
                File dataFile = File.createTempFile(StockPriceExtension.EXTENSION_ID, ".csv", environment.getTemporaryDirectory());
                dataFile.deleteOnExit();
                
                FileOutputStream csvResult = new FileOutputStream(dataFile);
                Writer outputWriter = new OutputStreamWriter(csvResult);
        		
        		CsvWriterSettings writerSettings = new CsvWriterSettings();
        		CsvWriter writer = new CsvWriter(outputWriter, writerSettings);

        		for (int i = 0; i < rows.size(); i++) {
        	        for(int j = 0; j < rows.get(i).length; j++){
        				writer.writeValue(rows.get(i)[j]);
        			}
        	        writer.writeValuesToRow();
        		}
        		writer.close();
                
                return dataFile;
            } catch (Exception e) {
                throw new DAException("StockPrice Extension acquisition failed" + e.toString(), e);
            }
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        }
    }

    private static class StockPriceExtensionMetadataRequestJob implements IDAEMetadataAcquisitionJob {
        IDAEAcquisitionState acquisitionState;

        StockPriceExtensionMetadataRequestJob (IDAEAcquisitionState acquisitionState) {
            this.acquisitionState = acquisitionState;
        }

        @Override
        public String execute(IDAEProgress callback) throws DAException {
            try {
                JSONObject infoJSON = new JSONObject(acquisitionState.getInfo());
                
                String metadata = new String(infoJSON.getString("docmetadata"));
                return metadata;
            } catch (Exception e) {
                throw new DAException("StockPrice Extension acquisition failed", e);
            }
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// Called once acquisition is complete
        }
    }

    private class StockPriceExtensionClientRequestJob implements IDAEClientRequestJob {

        String request;

        StockPriceExtensionClientRequestJob(String request) {
            this.request = request;
        }

        @Override
        public String execute(IDAEProgress callback) throws DAException {
            if ("ping".equals(request)) {
                return "pong";
            }
            return null;
        }

        @Override
        public void cancel() {
        	// Cancel is currently not supported
        }

        @Override
        public void cleanup() {
        	// This function is NOT called
        }

    }

    @Override
    public Set<DAEWorkflow> getEnabledWorkflows(IDAEAcquisitionState acquisitionState) {
    	// If the extension is incompatible with the current environment, it may disable itself using this function
    	// return EnumSet.allOf(DAEWorkflow.class) to enable the extension
    	// return EnumSet.noneOf(DAEWorkflow.class) to disable the extension
    	// Partial enabling is not currently supported
        return EnumSet.allOf(DAEWorkflow.class);
    }

}