package io.techasylum.kafka.statestore.document.no2;

import org.dizitart.no2.NitriteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public interface NitriteConfigSetter {
    Logger LOG = LoggerFactory.getLogger(NitriteConfigSetter.class);

    /**
     * Set the nitrite options for the provided storeName.
     *
     * @param storeName the name of the store being configured
     * @param builder   the nitrite builder
     * @param configs   the configuration supplied to {@link org.apache.kafka.streams.StreamsConfig}
     */
    void setConfig(final String storeName, final NitriteBuilder builder, final Map<String, Object> configs);

    void close(final String storeName, final NitriteBuilder builder);
}
