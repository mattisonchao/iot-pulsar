package io.iot.pulsar.test.env.mock;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.iot.pulsar.test.env.PulsarEnv;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * Base class for all tests that need a Pulsar instance without a ZK and BK cluster.
 */
@Slf4j
public class MockedPulsarService implements PulsarEnv {

    protected PulsarService pulsar;
    protected MockZooKeeper mockZooKeeper;
    protected MockZooKeeper mockZooKeeperGlobal;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected static final String CONFIG_CLUSTER_NAME = "iot-test";
    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private OrderedExecutor bkExecutor;

    @Nonnull
    @Override
    public String getBrokerUrl() {
        return pulsar.getBrokerServiceUrl();
    }

    @Nonnull
    @Override
    public String getServiceUrl() {
        return pulsar.getWebServiceAddress();
    }

    @Nonnull
    @Override
    public ServiceConfiguration getDefaultConfiguration() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setAdvertisedAddress("localhost");
        configuration.setClusterName(CONFIG_CLUSTER_NAME);
        // there are TLS tests in here, they need to use localhost because of the certificate
        configuration.setManagedLedgerCacheSizeMB(8);
        configuration.setActiveConsumerFailoverDelayTimeMillis(0);
        configuration.setDefaultNumberOfNamespaceBundles(1);
        configuration.setZookeeperServers("localhost:2181");
        configuration.setConfigurationStoreServers("localhost:3181");
        configuration.setAllowAutoTopicCreationType("non-partitioned");
        configuration.setBrokerShutdownTimeoutMs(0L);
        configuration.setBrokerServicePort(Optional.of(0));
        configuration.setBrokerServicePortTls(Optional.of(0));
        configuration.setWebServicePort(Optional.of(0));
        configuration.setWebServicePortTls(Optional.of(0));
        configuration.setBookkeeperClientExposeStatsToPrometheus(true);
        configuration.setNumExecutorThreadPoolSize(5);
        configuration.setBrokerMaxConnections(0);
        configuration.setBrokerMaxConnectionsPerIp(0);
        return configuration;
    }

    @Override
    @SneakyThrows
    public final void init(@Nonnull ServiceConfiguration serviceConfiguration) {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = OrderedExecutor.newBuilder().numThreads(1).name("mock-pulsar-bk").build();

        mockZooKeeper = createMockZooKeeper();
        mockZooKeeperGlobal = createMockZooKeeperGlobal();
        mockBookKeeper = createMockBookKeeper(bkExecutor);

        startBroker(serviceConfiguration);
        PulsarAdmin adminClient = pulsar.getAdminClient();
        setupDefaultTenantAndNamespace(adminClient);
    }

    @SneakyThrows
    @Override
    public final void cleanup() {
        if (pulsar != null) {
            stopBroker();
            pulsar = null;
        }
        if (mockBookKeeper != null) {
            mockBookKeeper.reallyShutdown();
            mockBookKeeper = null;
        }
        if (mockZooKeeperGlobal != null) {
            mockZooKeeperGlobal.shutdown();
            mockZooKeeperGlobal = null;
        }
        if (mockZooKeeper != null) {
            mockZooKeeper.shutdown();
            mockZooKeeper = null;
        }
        if (sameThreadOrderedSafeExecutor != null) {
            try {
                sameThreadOrderedSafeExecutor.shutdownNow();
                sameThreadOrderedSafeExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                log.error("sameThreadOrderedSafeExecutor shutdown had error", ex);
                Thread.currentThread().interrupt();
            }
            sameThreadOrderedSafeExecutor = null;
        }
        if (bkExecutor != null) {
            try {
                bkExecutor.shutdownNow();
                bkExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                log.error("bkExecutor shutdown had error", ex);
                Thread.currentThread().interrupt();
            }
            bkExecutor = null;
        }
    }

    protected void stopBroker() throws Exception {
        log.info("Stopping Pulsar broker. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
        // set shutdown timeout to 0 for forceful shutdown
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(0L);
        pulsar.close();
        pulsar = null;
    }

    public void startBroker(@Nonnull ServiceConfiguration serviceConfiguration) throws Exception {
        if (this.pulsar != null) {
            throw new RuntimeException("broker already started!");
        }
        serviceConfiguration.setBrokerShutdownTimeoutMs(0L);
        this.pulsar = spy(new PulsarService(serviceConfiguration));
        setupBrokerMocks(pulsar);
        pulsar.start();
        log.info("Pulsar started. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();
        doReturn(createLocalMetadataStore()).when(pulsar).createLocalMetadataStore();
        doReturn(createConfigurationMetadataStore()).when(pulsar).createConfigurationMetadataStore();

        Supplier<NamespaceService> namespaceServiceSupplier =
                () -> BrokerTestUtil.spyWithClassAndConstructorArgs(NamespaceService.class, pulsar);
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();

        doAnswer((invocation) -> spy(invocation.callRealMethod())).when(pulsar).newCompactor();
    }

    protected MetadataStoreExtended createLocalMetadataStore() {
        return new ZKMetadataStore(mockZooKeeper);
    }

    protected MetadataStoreExtended createConfigurationMetadataStore() {
        return new ZKMetadataStore(mockZooKeeperGlobal);
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
                CreateMode.PERSISTENT);
        return zk;
    }

    public static MockZooKeeper createMockZooKeeperGlobal() {
        return MockZooKeeper.newInstanceForGlobalZK(MoreExecutors.newDirectExecutorService());
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(OrderedExecutor executor) throws Exception {
        return BrokerTestUtil.spyWithClassAndConstructorArgs(NonClosableMockBookKeeper.class, executor);
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    private final BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties, StatsLogger statsLogger) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }
    };

    protected void setupDefaultTenantAndNamespace(@Nonnull PulsarAdmin admin) throws Exception {
        final String tenant = "public";
        final String namespace = tenant + "/default";

        if (!admin.clusters().getClusters().contains(CONFIG_CLUSTER_NAME)) {
            admin.clusters().createCluster(CONFIG_CLUSTER_NAME,
                    ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        }

        if (!admin.tenants().getTenants().contains(tenant)) {
            admin.tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(
                    Sets.newHashSet(CONFIG_CLUSTER_NAME)).build());
        }

        if (!admin.namespaces().getNamespaces(tenant).contains(namespace)) {
            admin.namespaces().createNamespace(namespace);
        }
    }
}
