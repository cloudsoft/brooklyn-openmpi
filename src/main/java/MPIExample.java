import MPI.MPICluster;
import MPI.MPINode;
import brooklyn.catalog.Catalog;
import brooklyn.catalog.CatalogConfig;
import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.StartableApplication;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.launcher.BrooklynLauncher;
import brooklyn.util.CommandLineUtil;
import com.google.common.collect.Lists;

import java.util.List;

//@Catalog(name="Open-MPI Cluster Demo", description="Deploy an Open-MPI cluster.")

public class MPIExample extends AbstractApplication {


    //@CatalogConfig(label="Initial Cluster Size (per location)")
    public static final ConfigKey<Integer> MPI_CLUSTER_SIZE = ConfigKeys.newConfigKey(
            "cassandra.cluster.initialSize", "Initial size of the MPI clusters", 1);

    public void init()
    {
        addChild(EntitySpec.create(MPICluster.class)
                .configure(MPICluster.INITIAL_SIZE, getConfig(MPI_CLUSTER_SIZE))
                .configure(MPICluster.MEMBER_SPEC, EntitySpec.create(MPINode.class)));
    }

    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port =CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", "localhost");

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
        .application(EntitySpec.create(StartableApplication.class, MPIExample.class).displayName("Brooklyn SGE with MPI example"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
        }
}