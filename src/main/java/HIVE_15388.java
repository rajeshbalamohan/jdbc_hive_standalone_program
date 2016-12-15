import java.sql.SQLException;

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
public class HIVE_15388 {

  private static final String baseSQL = "select * from HIVE_15388 where ";
  private static final String whereCondition = "i = 0";

  static String buildWhereCondition(String s, int count) {
    if (count == 0) {
      return s;
    }
    return buildWhereCondition("(" + s + ") OR " + whereCondition, (count - 1));
  }

  static String buildSQL(int count) {
    StringBuilder sb = new StringBuilder(whereCondition.length() * count);
    return sb.append(baseSQL).append(buildWhereCondition(whereCondition, count)).toString();
  }

  public static void main(String[] args) throws SQLException {
    JDBCExecutor executor = new JDBCExecutor(args[0], null, null);

    //Create table and test data
    executor.executeUpdate("DROP TABLE HIVE_15388");
    executor.executeUpdate("CREATE TABLE HIVE_15388 (i int)");
    executor.executeUpdate("INSERT INTO HIVE_15388 VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9)");

    for (int i = 1; i < 16; i++) {
      String sql = buildSQL(i);
      System.out.println("Iteration : " + i);
      System.out.println("SQL : " + sql);
      executor.executeStatement(sql);
      System.out.println();
      System.out.println();
    }
  }
}
