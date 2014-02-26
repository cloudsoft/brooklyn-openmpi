import MPI.MPICluster;
import MPI.MPINode;
import brooklyn.entity.BrooklynMgmtContextTestSupport;
import brooklyn.entity.basic.ApplicationBuilder;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.location.Location;
import brooklyn.location.LocationSpec;
import brooklyn.location.basic.LocalhostMachineProvisioningLocation;
import brooklyn.management.ManagementContext;
import brooklyn.test.entity.TestApplication;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MPIEntityTest extends BrooklynMgmtContextTestSupport {

    private static final Logger log = LoggerFactory.getLogger(MPIEntityTest.class);

    private TestApplication app;
    private Location testLocation;
    private MPICluster cluster;
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
    public void testInstallation()
    {
        cluster = app.createAndManageChild(EntitySpec.create(MPICluster.class)
                .configure(MPICluster.INITIAL_SIZE, 1)
                .configure(MPICluster.MEMBER_SPEC, EntitySpec.create(MPINode.class)));


        app.start(ImmutableList.of(testLocation));


        Entities.dumpInfo(app);
    }
}
