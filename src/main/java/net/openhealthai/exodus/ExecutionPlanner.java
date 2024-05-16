package net.openhealthai.exodus;

import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import net.openhealthai.exodus.config.structs.repositories.ResourceConstrainedDataRepository;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles concurrency and attempts to maximize resource usage via a modification of a maximum flow optimization problem
 * TODO finish implementation. Currently not used.
 */
public class ExecutionPlanner {

    private final SparkSession session;
    private final ExodusConfiguration config;
    private final Map<String, Integer> readThreadCapacities = new HashMap<>();
    private final Map<String, Integer> writeThreadCapacities = new HashMap<>();
    private final List<Edge> costs = new ArrayList<>();
    public ExecutionPlanner(SparkSession session, ExodusConfiguration config) {
        this.session = session;
        this.config = config;
        initializeThreadCapacities();
    }

    private void initializeThreadCapacities() {
        this.config.getRepositories().forEach(
                (repositoryId, definition) -> {
                    if (definition instanceof ResourceConstrainedDataRepository) {
                        if (((ResourceConstrainedDataRepository) definition).getMaxReadConnections() > 0) {
                            readThreadCapacities.put(repositoryId, ((ResourceConstrainedDataRepository) definition).getMaxReadConnections());
                        } else {
                            readThreadCapacities.put(repositoryId, Integer.MAX_VALUE);
                        }
                        if (((ResourceConstrainedDataRepository) definition).getMaxWriteConnections() > 0) {
                            writeThreadCapacities.put(repositoryId, ((ResourceConstrainedDataRepository) definition).getMaxWriteConnections());
                        } else {
                            writeThreadCapacities.put(repositoryId, Integer.MAX_VALUE);
                        }
                    } else {
                        readThreadCapacities.put(repositoryId, Integer.MAX_VALUE);
                        writeThreadCapacities.put(repositoryId, Integer.MAX_VALUE);
                    }
                }
        );
    }

    /**
     * Adds the given migration to the set of migrations to execute/the flow network
     * @param migration The migration to execute
     */
    public void scheduleMigration(DataMigrationDefinition migration) {

    }

    /**
     * Runs all scheduled migrations added via {@link #scheduleMigration(DataMigrationDefinition)}
     */
    public void executeMigrations() {
        session.logInfo(() -> "[Exodus]: Starting Migrations");

    }

    private class Edge {
        private String migrationId;
        private String sourceId;
        private String targetId;
        private Integer cost;

        public Edge(DataMigrationDefinition migration) {
            this.migrationId = migration.getIdentifier();
            this.sourceId = migration.getSourceRepositoryId();
            this.targetId = migration.getTargetRepositoryId();
            // Start with leaf node CPU count as default parallelism if not otherwise specified TODO this would be wrong for cluster environments
            this.cost = session.leafNodeDefaultParallelism();
        }
    }
}
