package com.ndl.bigquery;


import java.io.FileInputStream;
import java.io.FileWriter;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;


public class TestApp {
    public static void main(String... args) throws Exception {
        InputStream propertyFile = new FileInputStream("src/main/resources/config.properties");
        Properties prop = new Properties();
        prop.load(propertyFile);
        String query = prop.getProperty("query");
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        TableResult result = queryJob.getQueryResults();

        FileWriter fw = new FileWriter("src/main/resources/output.txt");

        // Print all pages of the results.
        for (FieldValueList row : result.iterateAll()) {
            // String type
            fw.write(String.valueOf(row));
            System.out.printf("Row value is : \n %s \n", row);
        }
        fw.close();
    }
}