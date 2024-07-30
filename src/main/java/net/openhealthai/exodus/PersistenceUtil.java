package net.openhealthai.exodus;

import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.CheckpointStatePersistence;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;

import java.sql.*;
import java.util.*;

public class PersistenceUtil {
    private final CheckpointStatePersistence persistence;
    private final String schema;
    private final HashSet<String> tablesExisting;

    public PersistenceUtil(ExodusConfiguration config) {
        this.persistence = config.getPersistence();
        this.schema = this.persistence.getSchema();
        this.tablesExisting = new HashSet<>();
    }

    public void init() {
        // Scan schema for checkpointed tables
        try (Connection conn = getPersistenceConnection()) {
            DatabaseMetaData metadata = conn.getMetaData();
            ResultSet rs = metadata.getTables(conn.getCatalog(), this.schema, "migration_%", null);
            while (rs.next()) {
                tablesExisting.add(rs.getString(3).toUpperCase(Locale.ROOT));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getPersistenceConnection() throws SQLException {
        return DriverManager.getConnection(this.persistence.getJdbcURL(), this.persistence.getJdbcUsername(), this.persistence.getJdbcPassword());
    }

    public List<Object> getCheckpointThresholds(DataMigrationDefinition migration) {
        if (!migration.isCheckpointed() || !migration.getCheckpointingStrategy().isPreDatafetchFilterStrategy()) {
            return Collections.emptyList();
        }
        try (Connection conn = getPersistenceConnection()) {
            String tableName = "migration_" + migration.getIdentifier();
            tableName = tableName.toUpperCase();
            if (!tablesExisting.contains(tableName)) {
                return Collections.emptyList();
            }
            String query = "SELECT $1 FROM $2.$3 ORDER BY EXODUS_CHECKPOINT_TIME DESC".replace("$2", schema).replace("$3", tableName);
            PreparedStatement ps = conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            ArrayList<Object> ret = new ArrayList<>();
            if (rs.next()) {
                for (int i = 0; i < columns; i++) {
                    ret.add(rs.getObject(i + 1)); // SQL columns 1-indexed
                }
            }
            return ret;
        } catch (SQLException e) {
            throw new RuntimeException("Error communicating with application persistence store", e);
        }
    }
}
