package org.example;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

// Press ⇧ twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:54032/sqlbackend120task";
        String user = "sanan";
        String password = "Test@123";

        try {
            try (Connection connection = DriverManager.getConnection(url, user, password);
                 Statement statement = connection.createStatement()) {

                String sql = "UPDATE ja_activity_log " +
                        "SET old_activity_code = ( " +
                        "    SELECT DISTINCT ON (object_id, DATE(TO_TIMESTAMP(op_date, 'YYYY-MM-DD HH24:MI:SS'))) activity_code " +
                        "    FROM hs_ja_activity " +
                        "    WHERE object_id = ja_activity_log.object_id " +
                        "      AND DATE(TO_TIMESTAMP(op_date, 'YYYY-MM-DD HH24:MI:SS')) = TO_DATE(SUBSTRING(date_time, 1, 8), 'YYYYMMDD') " +
                        "      AND op_type = 1 " +
                        "    ORDER BY object_id, DATE(TO_TIMESTAMP(op_date, 'YYYY-MM-DD HH24:MI:SS')), id " +
                        ") " +
                        "WHERE EXISTS ( " +
                        "    SELECT 1 " +
                        "    FROM hs_ja_activity " +
                        "    WHERE object_id = ja_activity_log.object_id " +
                        "      AND DATE(TO_TIMESTAMP(op_date, 'YYYY-MM-DD HH24:MI:SS')) = TO_DATE(SUBSTRING(date_time, 1, 8), 'YYYYMMDD') " +
                        "      AND op_type = 1 " +
                        ")";

                int rowsUpdated = statement.executeUpdate(sql);

                try (FileWriter fileWriter = new FileWriter("update_log.txt")) {
                    for (int i = 1; i <= rowsUpdated; i++) {
                        String updateSQL = "UPDATE ja_activity_log SET old_activity_code = '"
                                + getOldActivityCode(statement, i) + "' WHERE ID = '" + i + "';\n";
                        fileWriter.write(updateSQL);
                    }
                }

            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error: ", e);
        }
    }

    private static String getOldActivityCode(Statement statement, int id) throws Exception {
        String sql = "SELECT old_activity_code FROM ja_activity_log WHERE ID = '" + id + "'";
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                return resultSet.getString("old_activity_code");
            }
        }
        throw new Exception("Old activity code not found for ID: " + id);
    }
}