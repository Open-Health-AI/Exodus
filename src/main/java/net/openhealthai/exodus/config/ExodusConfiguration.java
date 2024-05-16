package net.openhealthai.exodus.config;

import net.openhealthai.exodus.config.structs.DataMigrationDefinition;
import net.openhealthai.exodus.config.structs.DataRepositoryDefinition;

import java.util.List;
import java.util.Map;

/**
 * A POJO for defining exodus data migration configuration settings
 * <br/>
 * Serialized/Deserialized via Jackson from JSON config
 */
public class ExodusConfiguration {

    private Map<String, DataRepositoryDefinition> repositories;
    private List<DataMigrationDefinition> migrations;

    public Map<String, DataRepositoryDefinition> getRepositories() {
        return repositories;
    }

    public void setRepositories(Map<String, DataRepositoryDefinition> repositories) {
        this.repositories = repositories;
    }

    public List<DataMigrationDefinition> getMigrations() {
        return migrations;
    }

    public void setMigrations(List<DataMigrationDefinition> migrations) {
        this.migrations = migrations;
    }
}
