package org.pyeval.config;

import org.apache.streampipes.config.SpConfig;

import static org.pyeval.config.ConfigKeys.*;

public enum Config implements PeConfigBackend {
    INSTANCE;

    private final static String SERVICE_ID = "pe/org.pyeval.processor.jvm";
    private final SpConfig config;

    Config() {
        config = SpConfig.getSpConfig(SERVICE_ID);
        config.register(HOST, "ExampleProcessor", "Data processor host");
        config.register(PORT, 6666, "Data processor port");
        config.register(SERVICE_NAME, "ExampleProcessor", "Data processor service name");
        config.register(BACKEND_HOST, "localhost", "Backend host");
        config.register(BACKEND_PORT, 8030, "Backend Port");
    }

    @Override
    public String getBackendHost() {
        return config.getString(BACKEND_HOST);
    }

    @Override
    public int getBackendPort() {
        return config.getInteger(BACKEND_PORT);
    }

    @Override
    public String getHost() {
        return config.getString(HOST);
    }

    @Override
    public int getPort() {
        return config.getInteger(PORT);
    }

    @Override
    public String getId() {
        return SERVICE_ID;
    }

    @Override
    public String getName() {
        return config.getString(SERVICE_NAME);
    }
}
