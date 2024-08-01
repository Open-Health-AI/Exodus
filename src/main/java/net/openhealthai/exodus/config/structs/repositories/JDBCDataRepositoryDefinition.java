package net.openhealthai.exodus.config.structs.repositories;

import net.openhealthai.exodus.CheckpointUtil;
import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.DataCheckpointingStrategy;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import net.openhealthai.exodus.config.structs.DataRepositoryDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class JDBCDataRepositoryDefinition extends DataRepositoryDefinition implements ResourceConstrainedDataRepository {
    private String jdbcURL;
    private String jdbcUsername;
    private String jdbcPassword;

    private boolean atomicReplacement = false;
    private String tempSchemaForAtomicReplacement;
    private int maxReadConnections = -1;
    private int maxWriteConnections = -1;

    public String getJdbcURL() {
        return jdbcURL;
    }

    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }

    public String getJdbcUsername() {
        return jdbcUsername;
    }

    public void setJdbcUsername(String jdbcUsername) {
        this.jdbcUsername = jdbcUsername;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public int getMaxReadConnections() {
        return maxReadConnections;
    }

    public void setMaxReadConnections(int maxReadConnections) {
        this.maxReadConnections = maxReadConnections;
    }

    public int getMaxWriteConnections() {
        return maxWriteConnections;
    }

    public void setMaxWriteConnections(int maxWriteConnections) {
        this.maxWriteConnections = maxWriteConnections;
    }

    public boolean isAtomicReplacement() {
        return atomicReplacement;
    }

    public void setAtomicReplacement(boolean atomicReplacement) {
        this.atomicReplacement = atomicReplacement;
    }

    public String getTempSchemaForAtomicReplacement() {
        return tempSchemaForAtomicReplacement;
    }

    public void setTempSchemaForAtomicReplacement(String tempSchemaForAtomicReplacement) {
        this.tempSchemaForAtomicReplacement = tempSchemaForAtomicReplacement;
    }

    @Override
    public Dataset<Row> read(SparkSession session, ExodusConfiguration config, DataMigrationDefinition callingMigration) {
        CheckpointUtil persistence = new CheckpointUtil(config);
        Properties connectionInfo = new Properties();
        connectionInfo.setProperty("user", this.getJdbcUsername());
        connectionInfo.setProperty("password", this.getJdbcPassword());
        String table = callingMigration.getSourcePath();
        // First, figure out how to partition/parallelism level
        int parallelism = -1;
        if (callingMigration.getPartitionColumn() == null) {
            session.logWarning(() -> "[Exodus Migration $1 - JDBC Read]: no partition column specified for read from $2. Falling back to single thread read".replace("$1", callingMigration.getIdentifier()).replace("$2", callingMigration.getSourcePath()));
            parallelism = 1;
        } else {
            parallelism = Math.max(callingMigration.getMaxReadConnections(), this.maxReadConnections);
            if (parallelism == -1) {
                parallelism = session.leafNodeDefaultParallelism(); // Silently default to max if no limits specified
            } else if (callingMigration.getMaxReadConnections() > this.maxReadConnections && this.maxReadConnections > 0) {
                parallelism = this.maxReadConnections;
                session.logWarning(() -> (
                        "[Exodus Migration $1 - JDBC Read]: $2 read connection limit $3 greater than data repository $4 " +
                                "defined max concurrent connection limit of $5. Using parallelism of $5"
                ).replace("$1", callingMigration.getIdentifier()
                ).replace("$2", callingMigration.getSourcePath()
                ).replace("$3", callingMigration.getMaxReadConnections() + ""
                ).replace("$4", callingMigration.getSourceRepositoryId()
                ).replace("$5", this.maxReadConnections + ""));
            }
        }
        if (parallelism > 1) {
            // Do a partitioned read
            String checkpointSuffix = "";
            Row params = persistence.getCheckpointThresholds(session, callingMigration);
            // - Adjust query for checkpointing if necessary
            if (callingMigration.isCheckpointed() && callingMigration.getCheckpointingStrategy().isPreDatafetchFilterStrategy()) {
                // - Get current param values
                if (params != null) {
                    StringBuilder suffixBuilder = new StringBuilder(" WHERE ");
                    boolean initFlag = false;
                    for (String column : callingMigration.getCheckpointColumns()) {
                        if (!initFlag) {
                            initFlag = true;
                        } else {
                            suffixBuilder.append("AND ");
                        }
                        suffixBuilder.append(column).append(" ");
                        if (callingMigration.getCheckpointingStrategy().equals(DataCheckpointingStrategy.MAX_VALUE)) { // Only MIN/MAX Value checkpoint strategies do this filtering
                            suffixBuilder.append("> ? ");
                        } else {
                            suffixBuilder.append("< ? ");
                        }
                    }
                    checkpointSuffix = suffixBuilder.toString();
                }
            }
            int type = -1;
            Object lower = null;
            Object upper = null;
            // - Determine lower and upper bound
            try (Connection conn = DriverManager.getConnection(this.getJdbcURL(), this.getJdbcUsername(), this.getJdbcPassword())) {
                String query = "SELECT MIN(" + callingMigration.getPartitionColumn() + ") AS idx_min,  MAX(" + callingMigration.getPartitionColumn() + ") AS idx_max FROM " + table + " exodus_read_" + UUID.randomUUID().toString().replaceAll("-", "") + checkpointSuffix;
                PreparedStatement ps = conn.prepareStatement(query);
                if (checkpointSuffix.trim().length() > 0) {
                    for (int i = 0; i < params.size(); i++) {
                        ps.setObject(i + 1, params.get(i)); // SQL is 1-indexed TODO fix param types
                    }
                }
                ResultSetMetaData queryMeta = ps.getMetaData();
                type = queryMeta.getColumnType(1);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    lower = rs.getObject("idx_min");
                    upper = rs.getObject("idx_max");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            // - Now define read itself using typecasts
            SimpleDateFormat dateSDF = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat timestampSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Dataset<Row> ret = null;
            switch (type) {
                case Types.SMALLINT:
                    ret = session
                            .read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", (Short) lower)
                            .option("upperBound", (Short) upper)
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.INTEGER:
                    ret = session
                            .read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", (Integer) lower)
                            .option("upperBound", (Integer) upper)
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.BIGINT:
                    ret = session
                            .read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", (Long) lower)
                            .option("upperBound", (Long) upper)
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.DECIMAL:
                    ret = session.read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", ((BigDecimal) lower).longValue())
                            .option("upperBound", ((BigDecimal) upper).longValue())
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.NUMERIC:
                    ret = session
                            .read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", ((Number) lower).longValue())
                            .option("upperBound", ((Number) upper).longValue())
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.DATE:
                    ret = session
                            .read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", dateSDF.format((Date) lower))
                            .option("upperBound", dateSDF.format((Date) upper))
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                case Types.TIMESTAMP:
                    ret = session.read()
                            .option("url", this.getJdbcURL())
                            .option("numPartitions", parallelism)
                            .option("partitionColumn", callingMigration.getPartitionColumn())
                            .option("lowerBound", timestampSDF.format((Timestamp) lower))
                            .option("upperBound", timestampSDF.format((Timestamp) upper))
                            .jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
                    break;
                default:
                    throw new UnsupportedOperationException("Column $1 is of an unsupported type for partitioning".replace("$1", callingMigration.getPartitionColumn()));
            }
            if (callingMigration.isCheckpointed() && callingMigration.getCheckpointingStrategy().equals(DataCheckpointingStrategy.KEY_VALUE_COMPARE)) {
                return persistence.retainNonExistingKV(callingMigration, ret, session);
            } else {
                return ret;
            }
        } else {
            Dataset<Row> ret = session.read().jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
            if (callingMigration.isCheckpointed() && callingMigration.getCheckpointingStrategy().equals(DataCheckpointingStrategy.KEY_VALUE_COMPARE)) {
                return persistence.retainNonExistingKV(callingMigration, ret, session);
            } else {
                return ret;
            }
        }
    }

    @Override
    public void write(SparkSession session, ExodusConfiguration config, Dataset<Row> data, DataMigrationDefinition callingMigration) {
        CheckpointUtil checkpointing = new CheckpointUtil(config);
        // First, figure out how to partition/parallelism level
        int parallelism = Math.max(callingMigration.getMaxWriteConnections(), this.maxWriteConnections);
        if (parallelism == -1) {
            parallelism = session.leafNodeDefaultParallelism(); // Silently default to max if no limits specified
        } else if (callingMigration.getMaxReadConnections() > this.maxWriteConnections && this.maxWriteConnections > 0) {
            parallelism = this.maxWriteConnections;
            session.logWarning(() -> (
                    "[Exodus Migration $1 - JDBC Write]: $2 write connection limit $3 greater than data repository $4 " +
                            "defined max concurrent connection limit of $5. Using parallelism of $5"
            ).replace("$1", callingMigration.getIdentifier()
            ).replace("$2", callingMigration.getSourcePath()
            ).replace("$3", callingMigration.getMaxReadConnections() + ""
            ).replace("4", callingMigration.getSourceRepositoryId()
            ).replace("$5", this.maxReadConnections + ""));
        }
        if (!callingMigration.isCheckpointed()) {
            // Truncate before write
            try (Connection conn =  DriverManager.getConnection(this.getJdbcURL(), this.getJdbcUsername(), this.getJdbcPassword())) {
                conn.prepareStatement("TRUNCATE TABLE " +  callingMigration.getSourcePath()).executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        Properties connectionInfo = new Properties();
        connectionInfo.setProperty("user", this.getJdbcUsername());
        connectionInfo.setProperty("password", this.getJdbcPassword());
        Dataset<Row> df = data.repartition(parallelism).persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        // First write to DB
        df.write()
                .option("compression", "snappy")
                .option("batchsize", "25000")
                .option("isolationLevel", "NONE")
                .mode(SaveMode.Append).jdbc(
                this.getJdbcURL(),
                callingMigration.getTargetPath(),
                connectionInfo
        );
        // And only once this is done do we write to persistence/checkpointing if applicable
        checkpointing.writeCheckpoint(callingMigration, df);
        // and now unpersist
        df.unpersist();
    }
}
