package net.openhealthai.exodus.config.structs;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import net.openhealthai.exodus.config.ExodusConfiguration;
import net.openhealthai.exodus.config.structs.repositories.JDBCDataRepositoryDefinition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A POJO for use within {@link net.openhealthai.exodus.config.ExodusConfiguration} to define a Data Repository.
 * <br/>
 * Serialized/Deserialized via Jackson from JSON config
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value= JDBCDataRepositoryDefinition.class, name = "JDBC")
})
public abstract class DataRepositoryDefinition {
    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public abstract Dataset<Row> read(SparkSession session, ExodusConfiguration config, DataMigrationDefinition callingMigration);

    public abstract void write(SparkSession session, ExodusConfiguration config, Dataset<Row> data, DataMigrationDefinition callingMigration);
}

