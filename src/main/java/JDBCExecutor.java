/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

public class JDBCExecutor {

  private final String connectURL;
  private final String userName;
  private final String password;

  private Stopwatch stopwatch;
  private final Stack<Long> stack = new Stack<>();

  private static final String CONNECT_URL = "connectUrl";
  private static final String USERNAME = "user";
  private static final String PASSWORD = "password";
  private static final String SQLFILE = "sqlFile";

  public JDBCExecutor(String connectURL, String userName, String password) throws SQLException {
    this.connectURL = connectURL;
    this.userName = userName;
    this.password = password;
    stopwatch = Stopwatch.createStarted();
  }

  private Connection getConnection() throws SQLException {
    Connection connection = DriverManager.getConnection(connectURL, userName, password);
    LOG("Time taken for getConnection : ");
    return connection;
  }

  public void executeSQLFile(File sqlFile) {
    Preconditions.checkArgument(sqlFile.exists() && sqlFile.isFile(),
        "Please provide valid file.." + sqlFile);
    try {
      List<String> sqlLines = Files.readAllLines(sqlFile.toPath());
      for (String sql : sqlLines) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
          sql = sql.substring(0, sql.lastIndexOf(";"));
        }
        if (!sql.isEmpty()) {
          executeStatement(sql);
          System.out.println();
        }
      }
    } catch (IOException e) {
      LOG("Error in processing file " + sqlFile, e);
    }
  }

  public void executeUpdate(String sql) throws SQLException {
    LOG("Executing query: " + sql);
    try (Connection connection = getConnection()) {
      Statement stmt = connection.createStatement();
      LOG("\t Time taken to create statement : ");

      stmt.executeUpdate(sql);
      LOG("\t Time taken to execute query : ");
    }
  }

  public void executeStatement(String sql) {

    LOG("Executing query: " + sql);
    try (Connection connection = getConnection()) {
      Statement stmt = connection.createStatement();
      LOG("\t Time taken to create statement : ");

      ResultSet rs = stmt.executeQuery(sql);
      LOG("\t Time taken to execute query : ");
      if (rs == null) {
        return;
      }

      ResultSetMetaData resultSetMetaData = rs.getMetaData();
      LOG("\t\t Time taken to get resultset metadata: ");

      boolean processedFirstRecord = false;
      while (rs.next()) {
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          Object colObj = rs.getObject(i);
        }
        if (!processedFirstRecord) {
          processedFirstRecord = true;
          LOG("\t\t Processed first record : ");
        }
      }
      LOG("\t\t Processed all records : ");

      rs.close();
      LOG("\t\t Closed resultSet : ");
    } catch (SQLException e) {
      LOG("Error executing query", e);
    }
    LOG("\t Closed connection : ");
  }

  static CommandLine pargeArgs(String[] args) throws ParseException {
    final Options opts = new Options();
    try {
      Option connectURL = OptionBuilder.withArgName(CONNECT_URL).withLongOpt(CONNECT_URL)
          .isRequired().hasArg().withDescription("Provide jdbc connectURL").create();
      Option userName = OptionBuilder.withArgName(USERNAME).withLongOpt(USERNAME)
          .isRequired(false).hasArg().withDescription("Provide userName").create();
      Option password = OptionBuilder.withArgName(PASSWORD).withLongOpt(PASSWORD)
          .isRequired(false).hasArg().withDescription("Provide password").create();
      Option sqlFile = OptionBuilder.withArgName(SQLFILE).withLongOpt(SQLFILE)
          .isRequired().hasArg().withDescription("Provide sqlFile").create();

      opts.addOption(connectURL).addOption(userName).addOption(password).addOption(sqlFile);

      CommandLineParser parser = new GnuParser();
      CommandLine commandLine = parser.parse(opts, args);

      return commandLine;
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("java -cp ./target/*:./target/lib/*: JDBCExecutor", opts);
      throw e;
    }
  }

  private void LOG(String msg) {
    LOG(msg, null);
  }

  private void LOG(String msg, Throwable t) {
    long diff = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    if (!stack.isEmpty()) {
      diff = diff - stack.pop();
    }
    System.out.println(msg + " (executed in " + diff + " ms)");
    if (t != null) {
      t.printStackTrace();
    }
    stack.push(stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = pargeArgs(args);

    JDBCExecutor executor = new JDBCExecutor(cmdLine.getOptionValue(CONNECT_URL),
        cmdLine.getOptionValue(USERNAME), cmdLine.getOptionValue(PASSWORD));
    executor.executeSQLFile(new File(cmdLine.getOptionValue(SQLFILE)));
  }
}
