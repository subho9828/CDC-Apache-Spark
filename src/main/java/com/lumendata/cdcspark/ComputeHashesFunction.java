package com.lumendata.cdcspark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class ComputeHashesFunction implements Function<Row, Row> {

	private static final long serialVersionUID = 2625706620240718439L;
	
	private List<ColumnMetadata> columnsMetadata;
	
	public ComputeHashesFunction(List<ColumnMetadata> columnsMetadata) {
		this.columnsMetadata = columnsMetadata;
	}

	@Override
	public Row call(Row row) throws Exception {
		
		StringBuilder key = new StringBuilder();
		StringBuilder nonKey = new StringBuilder();
		
		for (ColumnMetadata metadata : columnsMetadata) {
			if (!metadata.ignore()) {
				String val = row.getAs(metadata.getName());
				if (null == val) {
					val = "null";
				}
				
				if (metadata.isKey()) {
					key.append(val);
				} else {
					nonKey.append(val);
				}
			}
		}

		String keyHash = DigestUtils.md5Hex(key.toString()); 
		String nonKeyHash = DigestUtils.md5Hex(nonKey.toString());
		
		// Set the array list to the expected capacity. In Java 6 and earlier
		// we would have need to set initial capacity to 1.5x of expected capacity
		// to prevent growth to ensure capacity. Since Java 7 the array is grown
		// only if the size exceeds initial capacity.
		List<Object> rowVals = new ArrayList<Object>(row.size() + 2);
		// copy over existing data fields
		for (int i=0; i < row.size(); i++) {
			rowVals.add(row.get(i));
		}
		
		// Add the extra fields needed for CDC in the order expected (needs to match extended schema)
		rowVals.add(keyHash);
		rowVals.add(nonKeyHash);
		
		return RowFactory.create(rowVals.toArray());
	}
}
