import io.cloudsoft.hpc.sge.SgeCluster;
import io.cloudsoft.hpc.sge.SgeNode;
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
            "MPI.cluster.initialSize", "Initial size of the MPI clusters", 2);

    public void init()
    {
        addChild(EntitySpec.create(SgeCluster.class)
                .configure(SgeCluster.INITIAL_SIZE, getConfig(MPI_CLUSTER_SIZE))
                .configure(SgeCluster.MEMBER_SPEC, EntitySpec.create(SgeNode.class)));
    }

    public static void main(String[] argv) {
        List<String> args = Lists.newArrayList(argv);
        String port =CommandLineUtil.getCommandLineOption(args, "--port", "8081+");
        String location = CommandLineUtil.getCommandLineOption(args, "--location", "aws-ec2:us-west-2");

        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
        .application(EntitySpec.create(StartableApplication.class, MPIExample.class).displayName("Brooklyn io with MPI example"))
                .webconsolePort(port)
                .location(location)
                .start();

        Entities.dumpInfo(launcher.getApplications());
        }
}