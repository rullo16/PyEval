package org.pyeval.config;

import org.apache.streampipes.container.model.PeConfig;

public interface PeConfigBackend extends PeConfig {
    String getBackendHost();

    int getBackendPort();
}
