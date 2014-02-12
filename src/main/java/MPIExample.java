import MPI.MPICluster;
import MPI.MPINode;
import brooklyn.catalog.Catalog;
import brooklyn.catalog.CatalogConfig;
import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.proxying.EntitySpec;

@Catalog(name="Open-MPI Cluster Demo", description="Deploy an Open-MPI cluster.")

public class MPIExample extends AbstractApplication {


    @CatalogConfig(label="Initial Cluster Size (per location)", priority=1)
    public static final ConfigKey<Integer> MPI_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the Cassandra clusterss", 4);

    public void init()
    {
        addChild(EntitySpec.create(MPICluster.class)
                .configure(MPICluster.INITIAL_SIZE, getConfig(MPI_CLUSTER_SIZE))
                .configure(MPICluster.MEMBER_SPEC, EntitySpec.create(MPINode.class)));
    }
}