package com.lumendata.cdcspark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * CDC using Spark based on programming pattern and python implementation by Jeffrey Aven
 * https://github.com/avensolutions/cdc-at-scale-using-spark
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class CDCSparkApplication {
	
	private static final Logger logger = Logger.getLogger(CDCSparkApplication.class);
	
	private static final Metadata EMPTY_METADATA = new MetadataBuilder().build();
	
	private static final Pattern PARQUET_SPECIAL_CHARS = Pattern.compile("[\\s,;{}()\\n\\t\\r]+");
	
	private static final String PARQUET_COLUMN_PREFIX = "PCP";
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.out.println("Required application parameters: <yaml-configuration-file-path> <csv-file-path> <eff-start-date>");
			logger.error("Missing one or more application paramaters: <yaml-configuration-file-path> <csv-file-path> <eff-start-date>");
			return;
		}
		
		String configFile = args[0];
		String csvFile = args[1];
		String effectiveStartDate = args[2];
		
		CDCSparkApplication application = new CDCSparkApplication();
		application.processCSVFile(configFile, csvFile, effectiveStartDate);
	}
	
	public void processCSVFile(String configFile, String csvFile, String effeciveStartDate) {
		// Load the configuration file to process csv
		Configuration config = loadConfig(configFile);
		
		// Get the spark session:
		try (SparkSession session = SparkSession.
				builder().
				appName(config.getName()).
				master("local").
				getOrCreate();) 
		{	
			JavaSparkContext context = new JavaSparkContext(session.sparkContext());
			context.setLogLevel("WARN");
			
			Dataset<Row> initialDS;
			if (config.hasColumnHeaders()) {
				initialDS = session.read().
						option("header", "true").
						option("inferSchema", "false").
						csv(csvFile);
			} else {
				initialDS = session.read().
						option("header", "false").
						option("inferSchema", "false").
						schema(generateSchema(config)).
						csv(csvFile);
			}
			
			initialDS.show();
//			// If initialDS is empty do nothing and return
			
			// Now that we have read the file. Do some pre processing required for cdc. This will add 
			// more columns to the dataset. To facilitate that convert to RDD
			JavaRDD<Row> initialRDD = initialDS.javaRDD();
			PreprocessFunction preprocessFunction = new PreprocessFunction(config.getColumnsMetadata());
			JavaRDD<Row> hashedRDD = initialRDD.map(preprocessFunction);
			Dataset<Row> hashedDS = session.createDataFrame(hashedRDD, generateSchemaExtended(config));
			
			hashedDS.show();

			// Check if path to folder exists
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
			
			if (currentDS == null || currentDS.isEmpty()) {
				// Since we have no data, all records in CSV are considered inserts. Write out data.
				writeToCSV(hashedDS, config.hasColumnHeaders(), "target/output/csvoutput_insert");
				writeToParquet(renameColumnsToCurrent(hashedDS, config.getColumnsMetadata()), currentDSPath, true); // overwrite
			
			} else {
				System.out.println("We have current data");
				currentDS.show();
				
				// Since we have a current data set, this means we are processing the file to capture changed data.
				// Union the hashed dataset with the current dataset. 
				Dataset<Row> joinedDS = hashedDS.join(currentDS, hashedDS.col("keyhash").equalTo(currentDS.col("currentkeyhash")), "fullouter");
				
				joinedDS.persist();
				joinedDS.show();
				
				boolean overwrite = true;
				
				// INSERTS
				Dataset<Row> insertsDS = dropCurrentColumns(joinedDS.filter(joinedDS.col("currentkeyhash").isNull()), config.getColumnsMetadata());
				insertsDS.show();
				if (!insertsDS.isEmpty()) {
					writeToCSV(insertsDS, config.hasColumnHeaders(), "target/output/csvoutput_insert");
					writeToParquet(renameColumnsToCurrent(insertsDS, config.getColumnsMetadata()), currentDSPath + "_new", overwrite);
					overwrite = false;
				}
				
				// UPDATES
				// DEVNOTE: java developer take note that spark null semantics are similate to databases rather than java. This is why the 
				// filter below works to get only the updated records. If the null semantics of notEqual() method were similar to java then
				// we would have needed to have two additional filter before the one below, the first which would have filtered out all rows where
				// nonkeyhash was null and then the second to filter out all rows where currentnonkeyhash was null.
				// See https://spark.apache.org/docs/3.0.0/sql-ref-null-semantics.html
				Dataset<Row> updatesDS = dropCurrentColumns(joinedDS.filter(joinedDS.col("nonkeyhash").notEqual(joinedDS.col("currentnonkeyhash"))),
						config.getColumnsMetadata());
				updatesDS.show();
				if (!updatesDS.isEmpty()) {
					writeToCSV(updatesDS, config.hasColumnHeaders(), "target/output/csvoutput_update");
					writeToParquet(renameColumnsToCurrent(updatesDS, config.getColumnsMetadata()), currentDSPath + "_new", overwrite);
					overwrite = false;
				}
				
				// DELETES
				Dataset<Row> deletesDS = renameColumnsToInitial(
						dropInitialColumns(joinedDS.filter(joinedDS.col("keyhash").isNull()), config.getColumnsMetadata()), 
						config.getColumnsMetadata());
				deletesDS.show();
				if (!deletesDS.isEmpty()) {
					writeToCSV(deletesDS, config.hasColumnHeaders(), "target/output/csvoutput_delete");
					// We don't write deletes back to the parquet file
				}
				
				// NO CHANGE
				Dataset<Row> unchangedDS = dropInitialColumns(
						joinedDS.filter(joinedDS.col("nonkeyhash").equalTo(joinedDS.col("currentnonkeyhash"))), config.getColumnsMetadata());
				unchangedDS.show();
				if (!unchangedDS.isEmpty()) {
					// No need to generate csv for unchanged records. However write to the parquet file 
					// (Since we are overwriting the one from the previous run
					writeToParquet(unchangedDS, currentDSPath + "_new", overwrite);
					// No need to set overwrite = false since no further writes to parquet file
				}
				
				// Unpersist joinedDS
				joinedDS.unpersist();
			}
			
//			Scanner in = new Scanner(System.in);
//			System.out.println("Waiting to exit");
//			String pause = in.nextLine();
//			System.out.println(pause);
//			in.close();
			
//			File rename = new File(currentDSPath + "_new");
//			rename.renameTo(new File(currentDSPath + "_old"));

//			Dataset<Row> df2 = df.withColumn("keyhash", functions.lit("foo"));
//			df2.foreach((row) -> {System.out.println(row.toString() + " : " + row.getAs("Serial Number"));});
//			df2.show();
			
			System.out.println("DONE!!!");
		}
	}
	
	private String getParquetName(String colName) {
		return PARQUET_COLUMN_PREFIX + PARQUET_SPECIAL_CHARS.matcher(colName).replaceAll("_");
	}
	
	private Dataset<Row> dropInitialColumns(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// drop data columns
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.drop(colMetadata.getName());
		}
		// drop hash columns
		return dataset.drop("keyhash", "nonkeyhash");
	}
	
	private Dataset<Row> dropCurrentColumns(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// drop current data columns
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.drop(getParquetName(colMetadata.getName()));
		}
		// drop current hash columns
		return dataset.drop("currentkeyhash", "currentnonkeyhash");
	}
	
	private Dataset<Row> renameColumnsToInitial(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.withColumnRenamed(getParquetName(colMetadata.getName()), colMetadata.getName());
		}
		
		// Rename hash columns
		dataset = dataset.withColumnRenamed("currentkeyhash", "keyhash").withColumnRenamed("currentnonkeyhash", "nonkeyhash");
		
		return dataset;
	}
	
	private Dataset<Row> renameColumnsToCurrent(Dataset<Row> dataset, List<ColumnMetadata> columnsMetadata) {
		// Make column names parquet safe and prefix with a string to distinguish between column names from original(csv)
		// and column names from parquet files
		for (ColumnMetadata colMetadata : columnsMetadata) {
			dataset = dataset.withColumnRenamed(colMetadata.getName(), getParquetName(colMetadata.getName()));
		}
		
		// Rename hash columns
		dataset = dataset.withColumnRenamed("keyhash", "currentkeyhash").withColumnRenamed("nonkeyhash", "currentnonkeyhash");
		
		return dataset;
	}
	
	private void writeToCSV(Dataset<Row> dataset, boolean withHeaders, String path) {
		dataset.drop("keyhash", "nonkeyhash").coalesce(1).write().option("header", Boolean.toString(withHeaders)).csv(path);
	}
	
	private void writeToParquet(Dataset<Row> dataset, String path, boolean overwrite) {
		SaveMode mode = overwrite ? SaveMode.Overwrite : SaveMode.Append;
		dataset.write().mode(mode).option("header", "true").parquet(path);
	}
	
	private Configuration loadConfig(String configFile) {	
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
	
	private StructType generateSchemaExtended(Configuration config) {
		List<StructField> fieldList = getSchemaFields(config);
		// Add the extra columns required to calculate changes to schema
		fieldList.add(new StructField("keyhash", DataTypes.StringType, true, EMPTY_METADATA));
		fieldList.add(new StructField("nonkeyhash", DataTypes.StringType, true, EMPTY_METADATA));
		return new StructType(fieldList.toArray(new StructField[fieldList.size()]));
	}
	
	private StructType generateSchema(Configuration config) {
		List<StructField> fieldList = getSchemaFields(config);
		return new StructType(fieldList.toArray(new StructField[fieldList.size()]));
	}
	
	private List<StructField> getSchemaFields(Configuration config) {
		List<StructField> fieldList = new LinkedList<StructField>();
		for (ColumnMetadata column : config.getColumnsMetadata()) {
			fieldList.add(new StructField(column.getName(), DataTypes.StringType, true, EMPTY_METADATA));
		}
		return fieldList;
	}
}
