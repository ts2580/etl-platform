import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateCdcSlotRegistry {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mariadb://175.193.22.60:3308/config?characterEncoding=UTF-8";
        String user = "root";
        String password = "dbfltjdwls";
        String sql = "CREATE TABLE IF NOT EXISTS cdc_slot_registry ("
                + " id BIGINT NOT NULL AUTO_INCREMENT,"
                + " selected_object VARCHAR(255) NOT NULL,"
                + " is_active BOOLEAN NOT NULL DEFAULT TRUE,"
                + " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + " updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + " PRIMARY KEY (id),"
                + " UNIQUE KEY uk_cdc_slot_registry_selected_object (selected_object)"
                + ")";

        Class.forName("org.mariadb.jdbc.Driver");
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            System.out.println("cdc_slot_registry ensured");
        }
    }
}
