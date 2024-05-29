package net.openhealthai.exodus.config.structs.repositories;

import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import net.openhealthai.exodus.config.structs.DataRepositoryDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JDBCDataRepositoryDefinition extends DataRepositoryDefinition implements ResourceConstrainedDataRepository {
    private String driverClass;
    private String jdbcURL;
    private String jdbcUsername;
    private String jdbcPassword;
    private int maxReadConnections = -1;
    private int maxWriteConnections = -1;

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

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

    @Override
    public Dataset<Row> read(SparkSession session, DataMigrationDefinition callingMigration) {
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
                ).replace("4", callingMigration.getSourceRepositoryId()
                ).replace("$5", this.maxReadConnections + ""));
            }
        }
        if (parallelism > 1) {
            // Do a partitioned read
            session
                    .read()
                    .format("jdbc")
                    .option("url", this.getJdbcURL())
                    .option("partitionColumn", callingMigration.getPartitionColumn())
        } else {
            session.read().jdbc(this.getJdbcURL(), callingMigration.getSourcePath(), connectionInfo);
        }
        return null;
    }

    @Override
    public void write(SparkSession session, Dataset<Row> data, DataMigrationDefinition callingMigration) {

    }
}
