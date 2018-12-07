package idv.gecko;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class exporter {

    private static final Logger log = LoggerFactory.getLogger(exporter.class);

    public static void main(String[] args) {

        try {
            // Setup the connection with the DB (MySQL)
            String databaseName = "db_xx";
            String userName = "user_xx";
            String password = "pwd_ww";
            String port = "3306";
            String hostUrl = "127.0.0.1";

            Class.forName("com.mysql.jdbc.Driver");
//            Class.forName("com.mysql.cj.jdbc.Drive");

            StringBuffer sb = new StringBuffer();
            String connectionUrl = sb.append("jdbc:mysql://").append(hostUrl).append(":").append(port).append("/").append(databaseName).toString();

            Connection conn = DriverManager.getConnection(connectionUrl, userName, password);

            // --- LISTING DATABASE SCHEMA NAMES ---
            ResultSet resultSet = conn.getMetaData().getCatalogs();
            while (resultSet.next()) {
                log.info("Schema Name = " + resultSet.getString("TABLE_CAT"));
            }
            resultSet.close();

            // --- LISTING DATABASE TABLE NAMES ---
            String[] types = {"TABLE"};
            resultSet = conn.getMetaData()
                    .getTables(databaseName, null, "%", types);
            List<String> tables = new ArrayList<>();
            while (resultSet.next()) {
                tables.add(resultSet.getString(3));
            }
            resultSet.close();

            tables.forEach(tableName -> {
                try {
                    log.info("Table Name = " + tableName);

                    // --- LISTING DATABASE COLUMN NAMES ---
                    DatabaseMetaData meta = conn.getMetaData();
                    ResultSet resultSet2 = meta.getColumns(databaseName, null, tableName, "%");
                    while (resultSet2.next()) {
                        //                log.info("Column Name of table " + tableName + " = " + resultSet.getString(4));
                        log.info("  [      SCHEMA] table={}, column={}, typeName={}, columnSize={}, nullable={}, autoincrement={}, comment={}",
                                resultSet2.getString(3),
                                resultSet2.getString(4),
                                resultSet2.getString(6),
                                resultSet2.getString(7),
                                resultSet2.getBoolean(11),
                                resultSet2.getBoolean(23),
                                resultSet2.getString(12)
                        );
                    }
                    resultSet2.close();

                    ResultSet resultSet4 = meta.getPrimaryKeys("", databaseName, tableName);
                    while (resultSet4.next()) {
                        log.info("  [          PK] table={}, columnName={}, seqNo={}, pkName={}",
                                resultSet4.getString(3),
                                resultSet4.getString(4),
                                resultSet4.getString(5),
                                resultSet4.getString(6)
                        );
                    }
                    resultSet4.close();

                    ResultSet resultSet5 = meta.getIndexInfo("", databaseName, tableName, true, false);
                    while (resultSet5.next()) {
                        log.info("  [ UniqueIndex] table={}, columnName={}, nonUnique={}, sort={}",
                                resultSet5.getString(3),
                                resultSet5.getString(9),
                                resultSet5.getString(4),
                                StringUtils.equalsIgnoreCase(resultSet5.getString(10), "A") ? "ASC" : "DESC"
                        );
                    }
                    resultSet5.close();

                    ResultSet resultSet6 = meta.getIndexInfo("", databaseName, tableName, false, false);
                    while (resultSet6.next()) {
                        log.info("  [!UniqueIndex] table={}, columnName={}, nonUnique={}, sort={}",
                                resultSet6.getString(3),
                                resultSet6.getString(9),
                                resultSet6.getString(4),
                                StringUtils.equalsIgnoreCase(resultSet6.getString(10), "A") ? "ASC" : "DESC"
                        );
                    }
                    resultSet6.close();

                    tables.forEach(foreignTableName -> {

                        try {
                            // https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getCrossReference(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String)
                            ResultSet resultSet3 = meta.getCrossReference("", databaseName, tableName, "", databaseName, foreignTableName);
                            while (resultSet3.next()) {
                                log.info("  [          FK] fkTable={}, fkTable={}, fkColumn={}, updateRule={}, deleteRule={}",
                                        resultSet3.getString(6),
                                        resultSet3.getString(7),
                                        resultSet3.getString(8),
                                        getCascadeRule(resultSet3.getShort(10)),
                                        getCascadeRule(resultSet3.getShort(11))
                                );
                            }
                            resultSet3.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    });


                } catch (SQLException e) {
                    e.printStackTrace();
                }

            });

            resultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String getCascadeRule(short updateRule) {
        if (updateRule == DatabaseMetaData.importedKeyNoAction) {
            return "NO ACTION";
        } else if (updateRule == DatabaseMetaData.importedKeyCascade) {
            return "CASCADE";
        } else if (updateRule == DatabaseMetaData.importedKeySetNull) {
            return "SET NULL";
        } else if (updateRule == DatabaseMetaData.importedKeySetDefault) {
            return "SET DEFAULT";
        } else if (updateRule == DatabaseMetaData.importedKeyRestrict) {
            return "RESTRICT";
        }

        return "?";
    }
}
