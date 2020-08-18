package com.lumendata.cdcspark;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SchemaUtil {

	private static final Metadata EMPTY_METADATA = new MetadataBuilder().build();
	
	private SchemaUtil() {}
	
	// Static utility methods
	
	public static StructType generateSchemaExtended(Configuration config) {
		List<StructField> fieldList = getSchemaFields(config);
		// Add the extra columns required to calculate changes to schema
		fieldList.add(new StructField("keyhash", DataTypes.StringType, true, EMPTY_METADATA));
		fieldList.add(new StructField("nonkeyhash", DataTypes.StringType, true, EMPTY_METADATA));
		return new StructType(fieldList.toArray(new StructField[fieldList.size()]));
	}

	public static StructType generateSchema(Configuration config) {
		List<StructField> fieldList = getSchemaFields(config);
		return new StructType(fieldList.toArray(new StructField[fieldList.size()]));
	}

	private static List<StructField> getSchemaFields(Configuration config) {
		List<StructField> fieldList = new LinkedList<StructField>();
		for (ColumnMetadata column : config.getColumnsMetadata()) {
			fieldList.add(new StructField(column.getName(), DataTypes.StringType, true, EMPTY_METADATA));
		}
		return fieldList;
	}

}
