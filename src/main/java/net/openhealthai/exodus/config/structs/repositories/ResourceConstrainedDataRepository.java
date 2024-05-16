package net.openhealthai.exodus.config.structs.repositories;

/**
 * Implementations of this class are resource constrained in some way (e.g., maximum read thread count)
 */
public interface ResourceConstrainedDataRepository {
    int getMaxReadConnections();
    int getMaxWriteConnections();
}
