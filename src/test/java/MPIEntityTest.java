import io.cloudsoft.hpc.sge.SgeCluster;
import io.cloudsoft.hpc.sge.SgeNode;
import brooklyn.entity.BrooklynMgmtContextTestSupport;
import brooklyn.entity.basic.ApplicationBuilder;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.location.LocationSpec;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.NoMachinesAvailableException;
import brooklyn.location.basic.LocalhostMachineProvisioningLocation;
import brooklyn.management.ManagementContext;
import brooklyn.test.entity.TestApplication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MPIEntityTest extends BrooklynMgmtContextTestSupport {

    private static final Logger log = LoggerFactory.getLogger(MPIEntityTest.class);

    private TestApplication app;
    private MachineProvisioningLocation testLocation;
    private SgeCluster cluster;
    private ManagementContext managementContext;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        app = ApplicationBuilder.newManagedApp(TestApplication.class);
        managementContext = app.getManagementContext();
       testLocation = managementContext.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class));

    }


    @AfterMethod(alwaysRun = true)
    public void shutdown() {
        Entities.destroyAll(app.getManagementContext());
    }

    @Test(groups = "installation")
    public void testInstallation() throws NoMachinesAvailableException {
        cluster = app.createAndManageChild(EntitySpec.create(SgeCluster.class)
                .configure(SgeCluster.INITIAL_SIZE, 1)
                .configure(SgeCluster.MEMBER_SPEC, EntitySpec.create(SgeNode.class)));


        app.start(ImmutableList.of(testLocation.obtain(ImmutableMap.of())));


        Entities.dumpInfo(app);
    }
}
