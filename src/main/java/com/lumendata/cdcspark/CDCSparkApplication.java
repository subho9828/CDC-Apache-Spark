package com.lumendata.cdcspark;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.lumendata.cdcspark.impl.FileHelper;

/**
 * CDC using Spark based on programming pattern and python implementation by Jeffrey Aven
 * https://cloudywithachanceofbigdata.com/change-data-capture-at-scale-using-spark/
 * https://github.com/avensolutions/cdc-at-scale-using-spark
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class CDCSparkApplication {
	
	private static final Logger logger = Logger.getLogger(CDCSparkApplication.class);
	
	private static final Pattern PARQUET_SPECIAL_CHARS = Pattern.compile("[\\s,;{}()\\n\\t\\r]+");
	
	private static final String PARQUET_COLUMN_PREFIX = "PCP";
	
	private static final String KEY_HASH = "keyhash";
	
	private static final String NON_KEY_HASH = "nonkeyhash";
	
	private static final String CURR_KEY_HASH = "currentkeyhash";
	
	private static final String CURR_NON_KEY_HASH = "currentnonkeyhash";
	
	private SparkSession session;
	
	private Configuration config;
	
	private IOHelper ioHelper;
	
	public CDCSparkApplication(SparkSession session, Configuration config, IOHelper ioHelper) {
		this.session = session;
		this.config = config;
		this.ioHelper = ioHelper;
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.out.println("Required application parameters: <yaml-configuration-file-path> <csv-file-path>");
			logger.error("Missing one or more application paramaters: <yaml-configuration-file-path> <csv-file-path>");
			return;
		}
		
		String configFile = args[0];
		String csvFile = args[1];
		
		// Load configuration file
		Configuration config = loadConfig(configFile);
		// Get the spark session:
		try (SparkSession session = getSparkSession(config.getName())) 
		{	
			JavaSparkContext context = new JavaSparkContext(session.sparkContext());
			context.setLogLevel("WARN");

			IOHelper ioHelper = new FileHelper(session, config);
			CDCSparkApplication application = new CDCSparkApplication(session, config, ioHelper);
			application.processCSVFile(csvFile);
		}
	}
	
	public void processCSVFile(String csvFile) {
		// Load data from the csv file
		Dataset<Row> initialDS = ioHelper.readCSV(csvFile);

		// Now that we have read the file. Do some pre processing required for cdc. This will add 
		// more columns to the dataset. To facilitate that convert to RDD
		JavaRDD<Row> initialRDD = initialDS.javaRDD();
		ComputeHashesFunction computeHashesFunction = new ComputeHashesFunction(config.getColumnsMetadata());
		JavaRDD<Row> hashedRDD = initialRDD.map(computeHashesFunction);
		Dataset<Row> hashedDS = session.createDataFrame(hashedRDD, SchemaUtil.generateSchemaExtended(config));

		Dataset<Row> currentDS = ioHelper.readCurrentDataIfExists();

		if (currentDS == null || currentDS.isEmpty()) {
			// Since we have no data, all records in CSV are considered inserts. Write out data.
			ioHelper.writeCSV(dropInitialHashColumns(hashedDS), config.hasColumnHeaders(), "_inserted");
			ioHelper.writeToNewTable(renameColumnsToCurrent(hashedDS, config.getColumnsMetadata()), true); // overwrite

		} else {
			// Since we have a current data set, this means we are processing the file to capture changed data.
			// Union the hashed dataset with the current dataset. 
			Dataset<Row> joinedDS = hashedDS.join(currentDS, hashedDS.col(KEY_HASH).equalTo(currentDS.col(CURR_KEY_HASH)), "fullouter");
			joinedDS.persist();
			boolean overwrite = true;

			// INSERTS
			Dataset<Row> insertsDS = dropCurrentColumns(joinedDS.filter(joinedDS.col(CURR_KEY_HASH).isNull()), config.getColumnsMetadata());
			if (!insertsDS.isEmpty()) {
				ioHelper.writeCSV(dropInitialHashColumns(insertsDS), config.hasColumnHeaders(), "_inserted");
				ioHelper.writeToNewTable(renameColumnsToCurrent(insertsDS, config.getColumnsMetadata()), overwrite);
				overwrite = false;
			}

			// UPDATES
			// DEVNOTE: java developer take note that spark null semantics are similate to databases rather than java. This is why the 
			// filter below works to get only the updated records. If the null semantics of notEqual() method were similar to java then
			// we would have needed to have two additional filter before the one below, the first which would have filtered out all rows where
			// nonkeyhash was null and then the second to filter out all rows where currentnonkeyhash was null.
			// See https://spark.apache.org/docs/3.0.0/sql-ref-null-semantics.html
			Dataset<Row> updatesDS = dropCurrentColumns(joinedDS.filter(joinedDS.col(NON_KEY_HASH).notEqual(joinedDS.col(CURR_NON_KEY_HASH))),
					config.getColumnsMetadata());
			if (!updatesDS.isEmpty()) {
				ioHelper.writeCSV(dropInitialHashColumns(updatesDS), config.hasColumnHeaders(), "_updated");
				ioHelper.writeToNewTable(renameColumnsToCurrent(updatesDS, config.getColumnsMetadata()), overwrite);
				overwrite = false;
			}

			// DELETES
			Dataset<Row> deletesDS = renameColumnsToInitial(
					dropInitialColumns(joinedDS.filter(joinedDS.col(KEY_HASH).isNull()), config.getColumnsMetadata()), 
					config.getColumnsMetadata());
			if (!deletesDS.isEmpty()) {
				ioHelper.writeCSV(dropInitialHashColumns(deletesDS), config.hasColumnHeaders(), "_deleted");
				// We don't write deletes back to the parquet file
			}

			// NO CHANGE
			Dataset<Row> unchangedDS = dropInitialColumns(
					joinedDS.filter(joinedDS.col(NON_KEY_HASH).equalTo(joinedDS.col(CURR_NON_KEY_HASH))), config.getColumnsMetadata());
			if (!unchangedDS.isEmpty()) {
				// (Since we are overwriting the one from the previous run
				ioHelper.writeToNewTable(unchangedDS, overwrite);
				// No need to set overwrite = false since no further writes to parquet file
			}

			// Unpersist joinedDS
			joinedDS.unpersist();
		}
		
		ioHelper.renameNewTablePathToCurrent();
	}
	
	public static SparkSession getSparkSession(String appName) {
		return SparkSession.
			builder().
			appName(appName).
			getOrCreate();
	}
	
	public static Configuration loadConfig(String configFile) {	
		Configuration configuration = null;
		try (InputStream inputStream = new FileInputStream(configFile);) {
			Yaml yaml = new Yaml(new Constructor(Configuration.class));
			configuration = yaml.load(inputStream);
		} catch (IOException e) {
			String errMsg = "Error loading configuration from file.";
			logger.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
		return configuration;
	}
		
	// Private static utility methods
	
	private static String getParquetName(String colName) {
		return PARQUET_COLUMN_PREFIX + PARQUET_SPECIAL_CHARS.matcher(colName).replaceAll("_");
	}
	
	private static Dataset<Row> dropInitialHashColumns(Dataset<Row> dataset) {
		return dataset.drop(KEY_HASH, NON_KEY_HASH);
	}
	
	private static Dataset<Row> dropInitialColumns(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// drop data columns
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.drop(colMetadata.getName());
		}
		// drop hash columns
		return dropInitialHashColumns(dataset);
	}
	
	private static Dataset<Row> dropCurrentColumns(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// drop current data columns
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.drop(getParquetName(colMetadata.getName()));
		}
		// drop current hash columns
		return dataset.drop(CURR_KEY_HASH, CURR_NON_KEY_HASH);
	}
	
	private static Dataset<Row> renameColumnsToInitial(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.withColumnRenamed(getParquetName(colMetadata.getName()), colMetadata.getName());
		}
		
		// Rename hash columns
		dataset = dataset.withColumnRenamed(CURR_KEY_HASH, KEY_HASH).withColumnRenamed(CURR_NON_KEY_HASH, NON_KEY_HASH);
		
		return dataset;
	}
	
	private static Dataset<Row> renameColumnsToCurrent(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// Make column names parquet safe and prefix with a string to distinguish between column names from original(csv)
		// and column names from parquet files
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.withColumnRenamed(colMetadata.getName(), getParquetName(colMetadata.getName()));
		}
		
		// Rename hash columns
		dataset = dataset.withColumnRenamed(KEY_HASH, CURR_KEY_HASH).withColumnRenamed(NON_KEY_HASH, CURR_NON_KEY_HASH);
		
		return dataset;
	}
}
