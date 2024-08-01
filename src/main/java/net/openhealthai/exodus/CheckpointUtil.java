package net.openhealthai.exodus;

import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.DataCheckpointingStrategy;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import org.apache.spark.sql.*;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class CheckpointUtil {
    private final String persistence;
    private final HashSet<String> tablesExisting;

    public CheckpointUtil(ExodusConfiguration config) {
        this.persistence = config.getCheckpointPath();
        this.tablesExisting = new HashSet<>();
        new File(this.persistence).mkdirs();
        for (File f : Objects.requireNonNull(new File(persistence).listFiles())) {
            this.tablesExisting.add(f.getName());
        }
    }

    private String getPersistenceTableName(DataMigrationDefinition migration) {
        String tableName = "migration_" + migration.getIdentifier();
        tableName = tableName.toUpperCase();
        return tableName;
    }

    private boolean shouldFilter(DataMigrationDefinition migration) {
        if (!migration.isCheckpointed()) {
            return false;
        }
        if (!tablesExisting.contains(this.getPersistenceTableName(migration))) {
            return false;
        }
        return true;
    }

    public Row getCheckpointThresholds(SparkSession spark, DataMigrationDefinition migration) {
        if (!shouldFilter(migration) || !migration.getCheckpointingStrategy().isPreDatafetchFilterStrategy()) {
            return null;
        }

        Row read = spark.read().parquet(this.persistence + "/" + this.getPersistenceTableName(migration)).first();
        return read;
    }

    /**
     * Retains only the entries in the parameter dataset that do not already exist in the persistence store
     * as defined by the parameter migration
     *
     * @param migration The migration requesting checkpointing services
     * @param df        The dataset to filter
     * @param spark     The spark session running the calling migration
     * @return The filtered dataset
     */
    public Dataset<Row> retainNonExistingKV(DataMigrationDefinition migration, Dataset<Row> df, SparkSession spark) {
        if (!shouldFilter(migration)) {
            return df;
        }
        String tableName = this.getPersistenceTableName(migration);
        Dataset<Row> filterDataset = spark.read().parquet(this.persistence + "/" + tableName);
        Column joinCondition = Arrays.stream(filterDataset.columns()).map(colName -> df.col(colName).equalTo(filterDataset.col(colName))).reduce(Column::and).orElseThrow(() -> new RuntimeException("KV Mapping Attempted with Empty Join Column Set"));
        return df.join(filterDataset, joinCondition, "leftsemi");
    }

    /**
     * Writes the checkpointing information associated with the given dataset to the persistence data store.
     * <br/>
     * An assumption is made that only new data being written is being passed to this function (i.e. data from prior
     * checkpoint is already excluded)
     * @param migration The migration being run
     * @param df    The dataframe to checkpoint
     */
    public void writeCheckpoint(DataMigrationDefinition migration, Dataset<Row> df) {
        if (!migration.isCheckpointed()) {
            return;
        }
        String tableName = getPersistenceTableName(migration);
        Dataset<Row> write;
        switch (migration.getCheckpointingStrategy()) {
            case MIN_VALUE, MAX_VALUE -> {
                write = getCheckpointMinMax(df, migration);
            }
            case KEY_VALUE_COMPARE -> {
                write = getCheckpointKV(df, migration);
            }
            default -> throw new RuntimeException("Unknown checkpointing strategy " + migration.getCheckpointingStrategy().name());
        }
        SaveMode writeMode =
                (this.tablesExisting.contains(tableName) && migration.getCheckpointingStrategy().equals(DataCheckpointingStrategy.KEY_VALUE_COMPARE))
                        ? SaveMode.Append : SaveMode.Overwrite;
        write.write().mode(writeMode).parquet(this.persistence + "/" + tableName);
        tablesExisting.add(tableName);
    }

    private Dataset<Row> getCheckpointKV(Dataset<Row> df, DataMigrationDefinition migration) {
        return df.select(migration.getCheckpointColumns().stream().map(df::col).toArray(Column[]::new));
    }

    private Dataset<Row> getCheckpointMinMax(Dataset<Row> df, DataMigrationDefinition migration) {
        Dataset<Row> ret = df.select(migration.getCheckpointColumns().stream().map(df::col).collect(Collectors.toCollection(ArrayList::new)).toArray(Column[]::new));
        DataCheckpointingStrategy strategy = migration.getCheckpointingStrategy();
        return df.sparkSession().createDataFrame(new ArrayList<>(Collections.singletonList(ret.orderBy(migration.getCheckpointColumns().stream().map(s -> {
            if (strategy.equals(DataCheckpointingStrategy.MAX_VALUE)) {
                return functions.desc(s);
            } else {
                return functions.asc(s);
            }
        }).toList().toArray(Column[]::new)).first())), df.schema());
    }

}
