package net.openhealthai.exodus.config.structs;

public class CheckpointStatePersistence {
    private String jdbcURL;
    private String jdbcUsername;
    private String jdbcPassword;
    private String schema;

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

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
