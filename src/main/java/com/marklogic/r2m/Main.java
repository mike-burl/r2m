package com.marklogic.r2m;

import org.apache.commons.cli.*;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

	public static void main(String[] args) throws Exception {
		System.out.println("r2m rocks!");

		Options options = new Options();

		Option dbOpt = new Option("db", "dbConnection", true, "DB Connection string");
		dbOpt.setRequired(true);
		options.addOption(dbOpt);

		Option userOpt = new Option("u", "user", true, "DB User");
		userOpt.setRequired(false);
		options.addOption(userOpt);

		Option pwdOpt = new Option("p", "password", true, "DB password");
		pwdOpt.setRequired(false);
		options.addOption(pwdOpt);

		Option joinOpt = new Option("jc", "joincfg", true, "Join Config File Path");
		joinOpt.setRequired(true);
		options.addOption(joinOpt);

		Option insertCfgOpt = new Option("mi", "mlInsert", true, "MarkLogic Insert Config File Path");
		insertCfgOpt.setRequired(true);
		options.addOption(insertCfgOpt);

		Option mlcfgOpt = new Option("mc", "mlConfig", true, "MarkLogic Config File Path");
		mlcfgOpt.setRequired(true);
		options.addOption(mlcfgOpt);

		Option batchSizeOpt = new Option("b", "batchSize", true, "Batch size per join");
		batchSizeOpt.setRequired(false);
		options.addOption(batchSizeOpt);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		try {
			CommandLine cmd = parser.parse(options, args);
			System.out.println("Starting main with "+args);

			String dbConnectionString = cmd.getOptionValue("dbConnection");
			String dbUser = "";
			if (cmd.hasOption("user")) {
				dbUser = cmd.getOptionValue("user");
			}
			String dbPassword = "";
			if (cmd.hasOption("user")) {
				dbPassword = cmd.getOptionValue("password");
			}
			String joinConfigFilePath = cmd.getOptionValue("joincfg");
			String mlInsertConfigFilePath = cmd.getOptionValue("mlInsert");
			System.out.println("Option value gives mlInsertConfigFilePath: "+mlInsertConfigFilePath);			String marklogicConfigFilePath = cmd.getOptionValue("mlConfig");

			String joinConfigJson = new String(Files.readAllBytes(Paths.get(joinConfigFilePath)));
			String mlInsertConfigJson = new String(Files.readAllBytes(Paths.get(mlInsertConfigFilePath)));
			System.out.println("Inside main: "+mlInsertConfigFilePath);
			String marklogicConfigJson = new String(Files.readAllBytes(Paths.get(marklogicConfigFilePath)));

			RelationalToMarkLogic r2m = new RelationalToMarkLogic();

			if (cmd.hasOption("batchSize")) {
				r2m.setJoinBatchSize(Integer.parseInt(cmd.getOptionValue("batchSize")));
			}

			r2m.setDBConnectionString(dbConnectionString);
			r2m.setDBUser(dbUser);
			r2m.setDBPassword(dbPassword);

			r2m.setJoinConfigJson(joinConfigJson);
			System.out.println("setting mlInsertConfigJson: "+mlInsertConfigJson);
			r2m.setMLInsertConfigJson(mlInsertConfigJson);
			r2m.setMarkLogicConfigJson(marklogicConfigJson);

			r2m.run();
		} catch (ParseException e) {
			formatter.printHelp("r2m", options);
		}
	}
}

