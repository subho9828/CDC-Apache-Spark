package com.lumendata.cdcspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public interface IOHelper {

	public Dataset<Row> readCSV(String csvFile);
	
	public Dataset<Row> readCurrentDataIfExists();
	
	public void writeCSV(Dataset<Row> dataset, boolean withHeaders, String path);
	
	public void writeToNewTable(Dataset<Row> dataset, boolean overwrite);
	
	public void renameNewTablePathToCurrent();
}
