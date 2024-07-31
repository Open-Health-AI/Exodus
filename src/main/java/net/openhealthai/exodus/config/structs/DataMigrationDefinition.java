package net.openhealthai.exodus.config.structs;

import java.util.ArrayList;
import java.util.List;

/**
 * A POJO for use within {@link net.openhealthai.exodus.config.ExodusConfiguration} to define a Data Migration Task.
 * <br/>
 * Serialized/Deserialized via Jackson from JSON config
 */
public class DataMigrationDefinition {
    private String identifier;
    private List<String> prerequisites = new ArrayList<>();
    private String sourceRepositoryId;
    private String targetRepositoryId;
    private String sourcePath;
    private String targetPath;
    private Boolean checkpointed;
    private List<String> checkpointColumns;
    private DataCheckpointingStrategy checkpointingStrategy;
    private Integer maxReadConnections = -1;
    private Integer maxWriteConnections = -1;
    private Integer writeBatchSizeRows = 10_000;
    private String partitionColumn;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public List<String> getPrerequisites() {
        return prerequisites;
    }

    public void setPrerequisites(List<String> prerequisites) {
        this.prerequisites = prerequisites;
    }

    public String getSourceRepositoryId() {
        return sourceRepositoryId;
    }

    public void setSourceRepositoryId(String sourceRepositoryId) {
        this.sourceRepositoryId = sourceRepositoryId;
    }

    public String getTargetRepositoryId() {
        return targetRepositoryId;
    }

    public void setTargetRepositoryId(String targetRepositoryId) {
        this.targetRepositoryId = targetRepositoryId;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public Boolean isCheckpointed() {
        return checkpointed;
    }

    public void setCheckpointed(Boolean checkpointed) {
        this.checkpointed = checkpointed;
    }

    public List<String> getCheckpointColumns() {
        return checkpointColumns;
    }

    public void setCheckpointColumns(List<String> checkpointColumn) {
        this.checkpointColumns = checkpointColumn;
    }

    public DataCheckpointingStrategy getCheckpointingStrategy() {
        return checkpointingStrategy;
    }

    public void setCheckpointingStrategy(DataCheckpointingStrategy checkpointingStrategy) {
        this.checkpointingStrategy = checkpointingStrategy;
    }

    public Integer getMaxReadConnections() {
        return maxReadConnections;
    }

    public void setMaxReadConnections(Integer maxReadConnections) {
        this.maxReadConnections = maxReadConnections;
    }

    public Integer getMaxWriteConnections() {
        return maxWriteConnections;
    }

    public void setMaxWriteConnections(Integer maxWriteConnections) {
        this.maxWriteConnections = maxWriteConnections;
    }

    public Integer getWriteBatchSizeRows() {
        return writeBatchSizeRows;
    }

    public void setWriteBatchSizeRows(Integer writeBatchSizeRows) {
        this.writeBatchSizeRows = writeBatchSizeRows;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }
}
