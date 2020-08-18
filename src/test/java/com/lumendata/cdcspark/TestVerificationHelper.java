package com.lumendata.cdcspark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class TestVerificationHelper {

	private static Map<String, CSVRecord> expectedRecordsMap;
	
	public TestVerificationHelper() {}
	
	/*
	 *  VERIFY OUTPUT FILE
	 */
	public void verifyOutputFile(String outputFile, String referenceCSVResource) throws IOException {
		URL referenceCSV = TestVerificationHelper.class.getClassLoader().getResource(referenceCSVResource);
		
		CSVParser recordsReference = CSVParser.parse(referenceCSV, Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());
		List<CSVRecord> recordsRef = recordsReference.getRecords();
		
		expectedRecordsMap = Collections.synchronizedMap(new HashMap<String, CSVRecord>());
		for (CSVRecord record : recordsRef) {
			expectedRecordsMap.put(record.get(0), record);
		}
		
		CSVParser recordsOutput = CSVParser.parse(new File(outputFile), Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());
		List<CSVRecord> recordsOut = recordsOutput.getRecords();

		for (CSVRecord record : recordsOut) {
			String key = record.get(0);
			CSVRecord recordRef = expectedRecordsMap.remove(key);
			if (null == recordRef) {
				fail(MessageFormat.format("Record with key {0} is not expected.", key));
			} else {
				for (int i = 0; i < recordRef.size(); i++) {
					String expected = recordRef.get(i);
					String value = record.get(i);
					assertEquals(expected, value);
				}
			}
		}
		
		// Make sure that no expected records left in map.
		assertTrue(expectedRecordsMap.isEmpty());
		
		recordsOutput.close();
		recordsReference.close();
	}
	
	/*
	 *  VERIFY DATASET
	 */
	public void verifyDataset(Dataset<Row> dataset, String referenceCSVResource) throws IOException {
		URL referenceCSV = TestVerificationHelper.class.getClassLoader().getResource(referenceCSVResource);
		
		CSVParser recordsMain = CSVParser.parse(referenceCSV, Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());
		List<CSVRecord> records = recordsMain.getRecords();
		
		expectedRecordsMap = Collections.synchronizedMap(new HashMap<String, CSVRecord>());
		for (CSVRecord record : records) {
			expectedRecordsMap.put(record.get(0), record);
		}

		dataset.foreach(row -> {
			String key = row.getString(0);
			CSVRecord record = expectedRecordsMap.remove(key);
			if (null == record) {
				fail(MessageFormat.format("Record with key {0} is not expected.", key));
			} else {
				for (int i = 0; i < record.size(); i++) {
					String expected = record.get(i);
					String value = row.getString(i);
					assertEquals(expected, value);
				}
			}
		});
		
		// Make sure that no expected records left in map.
		assertTrue(expectedRecordsMap.isEmpty());
	}

}
