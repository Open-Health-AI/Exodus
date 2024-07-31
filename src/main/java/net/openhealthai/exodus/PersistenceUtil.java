package net.openhealthai.exodus;

import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.CheckpointStatePersistence;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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

    private String getPersistenceTableName(DataMigrationDefinition migration) {
        String tableName = "migration_" + migration.getIdentifier();
        tableName = tableName.toUpperCase();
        return tableName;
    }

    private boolean shouldFilter(DataMigrationDefinition migration) {
        if (!migration.isCheckpointed() || !migration.getCheckpointingStrategy().isPreDatafetchFilterStrategy()) {
            return false;
        }
        if (!tablesExisting.contains(this.getPersistenceTableName(migration))) {
            return false;
        }
        return true;
    }

    public List<Object> getCheckpointThresholds(DataMigrationDefinition migration) {
        if (!shouldFilter(migration)) {
            return Collections.emptyList();
        }
        try (Connection conn = getPersistenceConnection()) {
            String query = "SELECT $1 FROM $2.$3 ORDER BY EXODUS_CHECKPOINT_TIME DESC".replace("$2", schema).replace("$3", getPersistenceTableName(migration));
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

    /**
     * Retains only the entries in the parameter dataset that do not already exist in the persistence store
     * as defined by the parameter migration
     *
     * @param migration The migration requesting persistence services
     * @param df        The dataset to filter
     * @param spark     The spark session running the calling migration
     * @return The filtered dataset
     */
    public Dataset<Row> retainNonExistingKV(DataMigrationDefinition migration, Dataset<Row> df, SparkSession spark) {
        if (!shouldFilter(migration)) {
            return df;
        }
        String tableName = this.getPersistenceTableName(migration);
        Properties properties = new Properties();
        properties.setProperty("user", this.persistence.getJdbcUsername());
        properties.setProperty("password", this.persistence.getJdbcPassword());
        Dataset<Row> filterDataset = spark.read().jdbc( // TODO implement parallel read
                this.persistence.getJdbcURL(),
                this.schema + "." + tableName,
                properties
        );
        Column joinCondition = Arrays.stream(filterDataset.columns()).map(colName -> df.col(colName).equalTo(filterDataset.col(colName))).reduce((c1, c2) -> c1.and(c2)).orElseThrow(() -> new RuntimeException("KV Mapping Attempted with Empty Join Column Set"));
        return df.join(filterDataset, joinCondition, "leftsemi");
    }
}
