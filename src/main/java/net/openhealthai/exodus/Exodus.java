package net.openhealthai.exodus;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Exodus {
    public static void main(String... args) throws IOException {
        // Initialize Configuration Settings
        if (args.length < 1) {
            throw new IllegalArgumentException("An argument specifying the path to the Exodus configuration is required");
        }
        ObjectMapper om = new ObjectMapper();
        ExodusConfiguration config = om.readValue(new File(args[0]), ExodusConfiguration.class);
        // Initialize Spark
        Date currDate = new Date(); // Timestamp for App name
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        SparkSession session = SparkSession.builder().appName("Exodus-" + sdf.format(currDate))
                .getOrCreate();
        // Generate an execution plan TODO no config validation yet.
        // TODO for now this is done in serial which is not optimal if we are not at max parallelism for every migration
        List<DataMigrationDefinition> migrations = config.getMigrations();
        for (DataMigrationDefinition migration : migrations) {
            executeMigration(session, migration, config);
        }
    }

    private static void executeMigration(SparkSession session, DataMigrationDefinition migration, ExodusConfiguration config) {
        session.sparkContext().setJobGroup("Exodus: " + migration.getIdentifier(), "Data Migration for Step " + migration.getIdentifier(), false);
        Dataset<Row> in  = config.getRepositories().get(migration.getSourceRepositoryId()).read(session, config, migration);
        config.getRepositories().get(migration.getTargetRepositoryId()).write(session, config, in, migration);
        session.sparkContext().clearJobGroup();
    }
}
