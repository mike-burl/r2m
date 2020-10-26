package com.marklogic.r2m;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class RelationalToMarkLogic {

	public RelationalToMarkLogic() {
		System.out.println("Initializing Jackson ObjectMapper");
		objectMapper = new ObjectMapper();

		if (sqlDateFormat != null) {
			System.out.println("Using format for serializing instances of java.sql.Date: " + sqlDateFormat);
			SimpleModule simpleModule = new SimpleModule();
			simpleModule.addSerializer(Date.class, new SqlDateSerializer(sqlDateFormat));
			objectMapper.registerModule(simpleModule);
		}
	}

	private Connection connection;

	// FSnow: pull from config, was NiFi processor property
	private static final String sqlDateFormat = "yyyy-MM-dd";

	// Three classes representing the JSON configuration files
	private TableQuery tableQuery;
	private MarkLogicConfiguration marklogicConfiguration;
	private MLInsertConfig mlInsertConfig;
	
	private String query;

	private BlockingQueue<String> docQueue;
	private BlockingQueue<List<Map<String, Object>>> rowQueue;
	private int numChildQueryExecutors = 10;	
	private ChildQueryExecutor childQueryExecutors[] = new ChildQueryExecutor[numChildQueryExecutors];
	private DocumentLoader docLoaders[];
	
	private String exitFlag = "Done";
	
	private ObjectMapper objectMapper = new ObjectMapper();
	
	public void setJoinBatchSize(int joinBatchSize) {
		this.joinBatchSize = joinBatchSize;
	}
	private int joinBatchSize = 100;
	
	public void setDBConnectionString(String dbConnectionString) {
		this.dbConnectionString = dbConnectionString;
	}
	private String dbConnectionString;
	
	public void setDBUser(String dbUser) {
		this.dbUser = dbUser;
	}
	private String dbUser;
	
	public void setDBPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	private String dbPassword;
	
	public void setJoinConfigJson(String joinConfigJson) {
		this.joinConfigJson = joinConfigJson;
	}
	private String joinConfigJson;
	
	public void setMLInsertConfigJson(String mlInsertConfigJson) {
		this.mlInsertConfigJson = mlInsertConfigJson;
	}
	private String mlInsertConfigJson;
	
	public void setMarkLogicConfigJson(String marklogicConfigJson) {
		this.marklogicConfigJson = marklogicConfigJson;
	}
	private String marklogicConfigJson;
	
	
	private void init() throws Exception
	{
		connection = DriverManager.getConnection(dbConnectionString, dbUser, dbPassword);

		try {
			tableQuery = objectMapper.readerFor(TableQuery.class).readValue(joinConfigJson);
		} catch (IOException e) {
			throw new Exception("Unable to read join configuration JSON: " + joinConfigJson, e);
		}

		try {
			mlInsertConfig = objectMapper.readerFor(MLInsertConfig.class).readValue(mlInsertConfigJson);
		} catch (IOException e) {
			throw new Exception("Unable to read MarkLogic insert configuration JSON: " + mlInsertConfigJson, e);
		}
		
		try {
			marklogicConfiguration = objectMapper.readerFor(MarkLogicConfiguration.class).readValue(marklogicConfigJson);
		} catch (IOException e) {
			throw new Exception("Unable to read MarkLogic configuration JSON: " + marklogicConfigJson, e);
		}
		
		query = tableQuery.getQuery();
		
		docQueue = new LinkedBlockingQueue<>(1000);
		// Each element will have a set of 100 rows for the child query executor
		// As such- we only need to have one ready for each thread to grab
		rowQueue = new LinkedBlockingQueue<>(numChildQueryExecutors);
	}
	
	public void run() throws Exception {
		
		init();
		
		final ColumnMapRowMapper rowMapper = new ColumnMapRowMapper();
		
		System.out.println("Batch size : " + joinBatchSize);
		System.out.println("# of child query executors : " + childQueryExecutors.length);

		createDocumentLoaders();
		createChildQueryExecutors();
		
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		int rowNumber = 0;
		int totalRows = 0;
		int printFrequency = numChildQueryExecutors * joinBatchSize;
		try {
			System.out.println("Executing query: " + query);

			try {
				preparedStatement = connection.prepareStatement(query);
				resultSet = preparedStatement.executeQuery();
			} catch (SQLException e) {
				System.out.println("Error in Executing Query "+query+" " +e.toString());
			};
				

			List<Map<String, Object>> columnMaps = new ArrayList<>();
			while (resultSet.next()) {
				columnMaps.add(rowMapper.mapRow(resultSet, rowNumber));
				
				rowNumber++;
				if (rowNumber >= joinBatchSize) {
					//System.out.println("Row number: " + rowNumber + "; Batch size: " + columnMaps.size() + "; Before executeChildQueries()");	
					executeChildQueries(columnMaps);
					//System.out.println("Row number: " + rowNumber + "; Before writeColumnMaps()");
					totalRows += rowNumber;
					if(totalRows % printFrequency == 0) {
						System.out.print("\rSent " + totalRows + " rows for processing.  Loader queue capacity: " + docQueue.remainingCapacity());
					}
					rowNumber = 0;
					columnMaps = new ArrayList<>();
				}
			}
			
			totalRows += rowNumber;


			// ResultSet is complete, so process final batch if it exists
			// Wait for the queue to empty and then terminate the child threads
			// Send 
			if (!columnMaps.isEmpty()) {
				System.out.println("Sending final batch of size: " + columnMaps.size());
				executeChildQueries(columnMaps);
			}
		} catch (SQLException ex) {
			throw ex;
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// ignore
				}
			}
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// ignore
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
		// Watch both queues to see when they finally empty.  
		System.out.println("Placed " + totalRows + " documents in queue to be loaded");
		System.out.println("Shutting down child query executors...");
		Thread.sleep(3000);
		int childQueriesRemaining = 0;
		int remainingJobs = numChildQueryExecutors;
		do {
			childQueriesRemaining = rowQueue.remainingCapacity();
			if(childQueriesRemaining == numChildQueryExecutors) {
				System.out.println("Child query queue exhausted.  Sending signal to child query executors to shut down");
				for(int i = 0; i < childQueryExecutors.length; i++) {
					childQueryExecutors[i].stopRunning();
				}
			} else {
				if(remainingJobs > numChildQueryExecutors - childQueriesRemaining) {
					remainingJobs = numChildQueryExecutors - childQueriesRemaining;
					System.out.print("\r" + remainingJobs + " child executors still processing");
				}
				Thread.sleep(1000);
			}
		} while (childQueriesRemaining < numChildQueryExecutors);
		
		for(int i = 0; i < childQueryExecutors.length; i++) {
			System.out.print("\rWaiting for executor #" + i + " to shut down");
			while(!childQueryExecutors[i].doneRunning) {
				Thread.sleep(1000);
			}
		}
		
		System.out.println("Shutting down document loaders...");
		int docsRemaining = 0;
		do {
			docsRemaining = docQueue.remainingCapacity();
			if(docsRemaining == 1000) {
				System.out.println("Document queue exhausted.  Sending signal to document loaders to shut down");
				for(int i = 0; i < docLoaders.length; i++) {
					docLoaders[i].stopRunning();
				}
			} else {
				int remainingDocs = 1000 - docsRemaining;
				System.out.println(remainingDocs + " documents to be loaded");
				Thread.sleep(1000);
			}
		} while (docsRemaining < 1000);
		
		System.out.println("Done!");
	}

	protected void executeChildQueries(List<Map<String, Object>> columnMapList) {
		//System.out.println("Number of column maps received: " + columnMapList.size());
		try {
			rowQueue.put(columnMapList);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		//childQueryExecutor.executeChildQueries(connection, tableQuery, columnMapList);
	}
	
	private void createChildQueryExecutors() throws SQLException {
		for(int i = 0; i < childQueryExecutors.length; i++) {
			ChildQueryExecutor cqe = new ChildQueryExecutor(this.dbConnectionString, this.dbUser, this.dbPassword, tableQuery, rowQueue, docQueue);
			childQueryExecutors[i] = cqe;
			new Thread(cqe).start();
		}
	}
	
	private void createDocumentLoaders() 
	{
		List<String> hosts = marklogicConfiguration.getHosts();
		//System.out.println("INSIDE createDocumentLoaders with "+mlInsertConfigJson);
		// ArrayList<DocumentLoader> loaders = new ArrayList<DocumentLoader>();
		int i = 0;
		docLoaders = new DocumentLoader[hosts.size() * marklogicConfiguration.getNumThreadsPerHost()];
		for (String host : hosts) {
			for(int j = 0; j < marklogicConfiguration.getNumThreadsPerHost(); j++) {
				DocumentLoader loader = new DocumentLoader(docQueue, mlInsertConfigJson, host, marklogicConfiguration);
				docLoaders[i] = loader;
				new Thread(loader).start();
				i++;
			}
		}

	}
}

