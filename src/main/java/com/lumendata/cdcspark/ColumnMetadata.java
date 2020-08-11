package com.lumendata.cdcspark;

import java.io.Serializable;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class ColumnMetadata implements Serializable {

	private static final long serialVersionUID = -7891693101749410471L;

	private String name;
	
	private boolean ignore;
	
	private boolean key;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean ignore() {
		return ignore;
	}

	public void setIgnore(boolean ignore) {
		this.ignore = ignore;
	}

	public Boolean isKey() {
		return key;
	}

	public void setKey(boolean key) {
		this.key = key;
	}
}