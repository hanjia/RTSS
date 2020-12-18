package org.hanjia.rtss.db;

import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

public class DynamoDBConnector {

	public void testConnection() {
		final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(new ProfileCredentialsProvider("default"))
                .build();

		ListTablesRequest request;

		boolean more_tables = true;
		String last_name = null;

		while (more_tables) {

			if (last_name == null) {
				request = new ListTablesRequest().withLimit(10);
			} else {
				request = new ListTablesRequest().withLimit(10).withExclusiveStartTableName(last_name);
			}

			ListTablesResult table_list = ddb.listTables(request);
			List<String> table_names = table_list.getTableNames();

			if (table_names.size() > 0) {
				for (String cur_name : table_names) {
					System.out.format("* %s\n", cur_name);
				}
			} else {
				System.out.println("No tables found!");
				System.exit(0);
			}

			last_name = table_list.getLastEvaluatedTableName();
			if (last_name == null) {
				more_tables = false;
			}
		}
	}
	
	public static void main(String[] args) {
		DynamoDBConnector connector = new DynamoDBConnector();
		connector.testConnection();
	}

}
