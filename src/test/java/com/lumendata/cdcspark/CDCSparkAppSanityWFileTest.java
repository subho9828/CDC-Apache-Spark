package com.lumendata.cdcspark;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.lumendata.cdcspark.impl.FileHelper;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
@TestMethodOrder(OrderAnnotation.class)
public class CDCSparkAppSanityWFileTest {
	
	private static SparkSession session;
	
	private static Configuration config;
	
	private static FileHelper fileHelper;
	
	@Test
	@Order(1)
	public void testSanityRun1() throws URISyntaxException, IOException {
		
		URL inputResource = CDCSparkAppSanityWFileTest.class.getClassLoader().getResource("sanitytest/run_1_input.csv");
		String inputPath = Paths.get(inputResource.toURI()).toFile().getAbsolutePath();
		
		CDCSparkApplication cdcSparkApplication = new CDCSparkApplication(session, config, fileHelper);
		cdcSparkApplication.processCSVFile(inputPath);
		
		TestVerificationHelper verificationHelper = new TestVerificationHelper();
		// Verify Insert File
		verificationHelper.verifyOutputFile(config.getOutputCSVPathPrefix() + "_inserted" + ".csv", "sanitytest/run_1_input.csv");
		
		assertTrue(!(new File(config.getOutputCSVPathPrefix() + "_deleted" + ".csv")).exists());
		assertTrue(!(new File(config.getOutputCSVPathPrefix() + "_updated" + ".csv")).exists());

	}
	
	@Test
	@Order(2)
	public void testSanityRun2() throws URISyntaxException, IOException {
		
		URL inputResource = CDCSparkAppSanityWFileTest.class.getClassLoader().getResource("sanitytest/run_2_input.csv");
		String inputPath = Paths.get(inputResource.toURI()).toFile().getAbsolutePath();
		
		CDCSparkApplication cdcSparkApplication = new CDCSparkApplication(session, config, fileHelper);
		cdcSparkApplication.processCSVFile(inputPath);
		
		TestVerificationHelper verificationHelper = new TestVerificationHelper();
		// Verify Insert File
		verificationHelper.verifyOutputFile(config.getOutputCSVPathPrefix() + "_inserted" + ".csv", "sanitytest/run_2_inserted.csv");
		
		// Verify Update File
		verificationHelper.verifyOutputFile(config.getOutputCSVPathPrefix() + "_updated" + ".csv", "sanitytest/run_2_updated.csv");
		
		// Verify Deleted File
		verificationHelper.verifyOutputFile(config.getOutputCSVPathPrefix() + "_deleted" + ".csv", "sanitytest/run_2_deleted.csv");
	}
	
	@Test
	@Order(3)
	public void testSanityPostRun2() throws URISyntaxException, IOException {
		TestVerificationHelper verificationHelper = new TestVerificationHelper();
		Dataset<Row> currentDataset = fileHelper.readCurrentDataIfExists();
		verificationHelper.verifyDataset(currentDataset, "sanitytest/run_2_post.csv");
	}
	
	// SETUP and TEARDOWN METHODS
	@BeforeAll
	public static void setUpBeforeClass() throws Exception {
		session = SparkSession.
				builder().
				appName("CDC Spark Sanity Test").
				master("local").
				getOrCreate();
		URL configResource = CDCSparkAppSanityWFileTest.class.getClassLoader().getResource("sanitytest/config-book-catalog.yaml");
		String configFilePath = Paths.get(configResource.toURI()).toFile().getAbsolutePath();
		config = CDCSparkApplication.loadConfig(configFilePath);
		fileHelper = new FileHelper(session, config);
		
		// Clean up test directories if any lingering around from previous failed test run
		FileUtils.deleteQuietly(new File(config.getCurrentTablePath()).getParentFile());
		
		JavaSparkContext context = new JavaSparkContext(session.sparkContext());
		context.setLogLevel("ERROR");
	}

	@AfterAll
	public static void tearDownAfterClass() throws Exception {
		if (session != null) {
			session.close();
		}
		FileUtils.deleteQuietly(new File(config.getCurrentTablePath()).getParentFile());
	}

	@BeforeEach
	public void setUp() throws Exception {

	}

	@AfterEach
	public void tearDown() throws Exception {
		FileUtils.deleteQuietly(new File(config.getOutputCSVPathPrefix() + "_inserted" + ".csv"));
		FileUtils.deleteQuietly(new File(config.getOutputCSVPathPrefix() + "_updated" + ".csv"));
		FileUtils.deleteQuietly(new File(config.getOutputCSVPathPrefix() + "_deleted" + ".csv"));
	}
}
