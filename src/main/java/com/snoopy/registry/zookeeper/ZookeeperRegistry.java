package com.snoopy.registry.zookeeper;


import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.constans.GrpcConstants;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.ISubscribeCallback;
import com.snoopy.grpc.base.registry.RegistryServiceInfo;
import com.snoopy.grpc.base.registry.ShutDownHookManager;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:18
 */
public class ZookeeperRegistry implements IRegistry {
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private ZkClient zkClient;
    private Map<String, IZkChildListener> listenerMap = new HashMap<>();

    public ZookeeperRegistry(ZkClient zkClient, GrpcRegistryProperties grpcRegistryProperties) {
        this.zkClient = zkClient;
        String name = grpcRegistryProperties.getUsername();
        String pwd = grpcRegistryProperties.getPassword();
        if (StringUtils.hasText(name) && StringUtils.hasText(pwd)) {
            String authInfo = name + ":" + pwd;
            this.zkClient.addAuthInfo("digest", authInfo.getBytes());
        }
        ShutDownHookManager.registerShutdownHook(this);
    }

    private void createNode(RegistryServiceInfo serviceInfo, ZookeeperNodeType nodeType) {
        String nodeTypePath = serviceInfo.getPath() + GrpcConstants.PATH_SEPARATOR + nodeType.getValue();
        String nodePath = nodeTypePath + GrpcConstants.PATH_SEPARATOR + serviceInfo.getHostAndPort();

        if (!zkClient.exists(nodeTypePath)) {
            zkClient.createPersistent(nodeTypePath, true);
        }
        if (zkClient.exists(nodePath)) {
            zkClient.delete(nodePath);
        }
        zkClient.createEphemeral(nodePath, serviceInfo.generateData());
    }

    private void removeNode(RegistryServiceInfo serviceInfo, ZookeeperNodeType nodeType) {
        String nodePath = serviceInfo.getPath() + GrpcConstants.PATH_SEPARATOR + nodeType.getValue()
                + GrpcConstants.PATH_SEPARATOR + serviceInfo.getHostAndPort();
        if (zkClient.exists(nodePath)) {
            zkClient.delete(nodePath);
        }
    }

    private void notifyChange(String nodeTypePath, ISubscribeCallback subscribeCallback, List<String> currentChilds) {
        List<RegistryServiceInfo> serviceInfoList = (currentChilds != null && currentChilds.size() > 0) ?
                currentChilds.stream().map(currentChild -> {
                    String url = zkClient.readData(nodeTypePath + GrpcConstants.PATH_SEPARATOR + currentChild);
                    return new RegistryServiceInfo(url);
                }).collect(Collectors.toList()) : new ArrayList<>();
        subscribeCallback.handle(serviceInfoList);
    }

    @Override
    public void subscribe(RegistryServiceInfo serviceInfo, ISubscribeCallback subscribeCallback) {
        reentrantLock.lock();
        try {
            removeNode(serviceInfo, ZookeeperNodeType.CLIENT);
            createNode(serviceInfo, ZookeeperNodeType.CLIENT);
            String nodeTypePath = serviceInfo.getPath() + GrpcConstants.PATH_SEPARATOR
                    + ZookeeperNodeType.SERVER.getValue();

            List<String> currentChilds = zkClient.getChildren(nodeTypePath);
            notifyChange(nodeTypePath, subscribeCallback, currentChilds);

            IZkChildListener zkChildListener = listenerMap.get(nodeTypePath);
            if (zkChildListener == null) {
                zkChildListener = new IZkChildListener() {
                    @Override
                    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                        notifyChange(nodeTypePath, subscribeCallback, currentChilds);
                    }
                };
                listenerMap.put(nodeTypePath, zkChildListener);
            }
            zkClient.subscribeChildChanges(nodeTypePath, zkChildListener);
        } catch (Throwable e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] subscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unsubscribe(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            removeNode(serviceInfo, ZookeeperNodeType.CLIENT);
            String nodeTypePath = serviceInfo.getPath() + GrpcConstants.PATH_SEPARATOR
                    + ZookeeperNodeType.SERVER.getValue();
            IZkChildListener zkChildListener = listenerMap.get(nodeTypePath);
            if (zkChildListener != null) {
                zkClient.unsubscribeChildChanges(nodeTypePath, zkChildListener);
            }
        } catch (Throwable e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] unsubscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void register(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            unregister(serviceInfo);
            createNode(serviceInfo, ZookeeperNodeType.SERVER);
        } catch (Throwable e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] register failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unregister(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            removeNode(serviceInfo, ZookeeperNodeType.SERVER);
        } catch (Throwable e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] register failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        listenerMap.clear();
        if (zkClient != null) {
            zkClient.close();
        }
    }
}
