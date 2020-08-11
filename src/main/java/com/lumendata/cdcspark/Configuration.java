package com.lumendata.cdcspark;

import java.util.List;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class Configuration {

	private String name;
	
	private boolean columnHeaders;
	
	private String currentTablePath;
	
	private String newTablePath;
	
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
}


