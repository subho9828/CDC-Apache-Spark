package com.lumendata.cdcspark;

import java.util.List;

/**
 * 
 * NOTE: see sample config file in test resources folder
 * src/test/resources/sanitytest/config-book-catalog.yaml
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class Configuration {

	private String name;
	
	private boolean columnHeaders;
	
	private String currentTablePath;
	
	private String newTablePath;
	
	private String outputCSVPathPrefix;
	
	private List<ColumnMetadata> columnsMetadata;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<ColumnMetadata> getColumnsMetadata() {
		return columnsMetadata;
	}

	public void setColumnsMetadata(List<ColumnMetadata> columnsMetadata) {
		this.columnsMetadata = columnsMetadata;
	}

	public boolean hasColumnHeaders() {
		return columnHeaders;
	}

	public void setColumnHeaders(boolean columnHeaders) {
		this.columnHeaders = columnHeaders;
	}

	public String getCurrentTablePath() {
		return currentTablePath;
	}

	public void setCurrentTablePath(String currentTablePath) {
		this.currentTablePath = currentTablePath;
	}

	public String getNewTablePath() {
		return newTablePath;
	}

	public void setNewTablePath(String newTablePath) {
		this.newTablePath = newTablePath;
	}

	public String getOutputCSVPathPrefix() {
		return outputCSVPathPrefix;
	}

	public void setOutputCSVPathPrefix(String outputCSVPathPrefix) {
		this.outputCSVPathPrefix = outputCSVPathPrefix;
	}
}


