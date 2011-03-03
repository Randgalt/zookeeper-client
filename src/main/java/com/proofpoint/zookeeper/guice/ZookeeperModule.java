package com.proofpoint.zookeeper.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.zookeeper.ZookeeperClient;
import com.proofpoint.zookeeper.ZookeeperClientConfig;

public class ZookeeperModule
        extends AbstractModule
{
    @Override
    protected void configure()
    {
        binder().bind(ZookeeperClient.class).in(Scopes.SINGLETON);
        ConfigurationModule.bindConfig(binder()).to(ZookeeperClientConfig.class);
    }
}
