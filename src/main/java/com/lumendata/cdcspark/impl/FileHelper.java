package com.lumendata.cdcspark.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.lumendata.cdcspark.Configuration;
import com.lumendata.cdcspark.IOHelper;
import com.lumendata.cdcspark.SchemaUtil;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class FileHelper implements IOHelper {

	private SparkSession session;
	
	private Configuration config;

	public FileHelper(SparkSession session, Configuration config) {
		this.session = session;
		this.config = config;
	}
	
	public Dataset<Row> readCSV(String csvFile) {
		Dataset<Row> csvDataSet;
		if (config.hasColumnHeaders()) {
			csvDataSet = session.read().
					option("header", "true").
					option("inferSchema", "false").
					csv(csvFile);
		} else {
			csvDataSet = session.read().
					option("header", "false").
					option("inferSchema", "false").
					schema(SchemaUtil.generateSchema(config)).
					csv(csvFile);
		}
		
		return csvDataSet;
	}
	
	public Dataset<Row> readCurrentDataIfExists() {
		String currentDSPath = config.getCurrentTablePath();
		File currentDSDir = new File(currentDSPath);
		Dataset<Row> currentDS = null;
		if (currentDSDir.exists() && currentDSDir.isDirectory()) {
			// Check to see if we have a current data set.
			currentDS = session.read().
					option("header", "true").
					option("inferSchema", "false").
					parquet(config.getCurrentTablePath());
		}
		return currentDS;
	}
	
	public void writeCSV(Dataset<Row> dataset, boolean withHeaders, String suffix) {
		dataset.coalesce(1).write().option("header", Boolean.toString(withHeaders)).csv(config.getOutputCSVPathPrefix() + suffix);
		moveCSVFiles(config.getOutputCSVPathPrefix() + suffix);
	}
	
	public void writeToNewTable(Dataset<Row> dataset, boolean overwrite) {
		SaveMode mode = overwrite ? SaveMode.Overwrite : SaveMode.Append;
		dataset.write().mode(mode).option("header", "true").parquet(config.getNewTablePath());
	}
	
	public void renameNewTablePathToCurrent() {
		try {
			// First delete current Table path, we no longer need it. It could also be renamed to 
			// keep it for archival, perhaps appended by timestamp for last n runs
			File currentTablePath = new File(config.getCurrentTablePath());
			// Additional checks for file existance and if file is a dir not necessary as this is a 
			// controlled environment. If exceptions propogate up to be known
			FileUtils.deleteDirectory(currentTablePath);
			// Rename newTablePath
			File newTablePath = new File(config.getNewTablePath());
			FileUtils.moveDirectory(newTablePath, currentTablePath);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	
	private void moveCSVFiles(String directory) {
		File dir = new File(directory);
		if (dir.exists()) {
			if (dir.isDirectory()) {
				File[] csvFiles = dir.listFiles((dirFile, name) -> {
					if (name.toLowerCase().endsWith(".csv")) {
						return true;
					}
					return false;
				});

				if (csvFiles.length == 1) {
					File csvFile = csvFiles[0];
					try {
						FileUtils.moveFile(csvFile, new File(directory + ".csv"));
						FileUtils.deleteDirectory(dir);
					} catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				} else {
					throw new RuntimeException();
				}
			} else {
				throw new RuntimeException();
			}
		}
	}

}
