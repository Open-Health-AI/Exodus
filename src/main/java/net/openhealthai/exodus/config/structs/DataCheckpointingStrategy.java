package net.openhealthai.exodus.config.structs;

public enum DataCheckpointingStrategy {
    MIN_VALUE,
    MAX_VALUE,
    KEY_VALUE_COMPARE;

    public boolean isPreDatafetchFilterStrategy() {
        return this.equals(DataCheckpointingStrategy.MIN_VALUE) || this.equals(DataCheckpointingStrategy.MAX_VALUE);
    }
}
