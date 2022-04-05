package com.snoopy.registry.zookeeper;

import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.IRegistryProvider;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.util.StringUtils;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:44
 */
public class ZookeeperRegistryProvider implements IRegistryProvider {
    public static final String REGISTRY_PROTOCOL_ZOOKEEPER = "zookeeper";
    private static final int DEFAULT_SESSION_TIMEOUT = 60_000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 60_000;

    @Override
    public IRegistry newRegistryInstance(GrpcRegistryProperties grpcRegistryProperties) {
        String sessionTimeout = grpcRegistryProperties.getExtra("sessionTimeout");
        String connectionTimeout = grpcRegistryProperties.getExtra("connectionTimeout");
        ZkClient zkClient = new ZkClient(
                grpcRegistryProperties.getAddress(),
                StringUtils.hasText(sessionTimeout) ? Integer.valueOf(sessionTimeout) : DEFAULT_SESSION_TIMEOUT,
                StringUtils.hasText(connectionTimeout) ? Integer.valueOf(connectionTimeout) : DEFAULT_CONNECTION_TIMEOUT
        );
        return new ZookeeperRegistry(zkClient, grpcRegistryProperties);
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_ZOOKEEPER;
    }
}
