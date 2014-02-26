package MPI;

import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.entity.basic.VanillaSoftwareProcessImpl;
import brooklyn.entity.java.VanillaJavaAppImpl;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPINodeImpl extends VanillaSoftwareProcessImpl implements MPINode {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPINodeImpl.class);
    private AtomicBoolean setNoOfProcessors;


    @Override
    public void init() {
        super.init();

    }


    @Override
    public void connectSensors() {
        super.connectSensors();
        connectServiceUpIsRunning();

    }

    @Override
    public void disconnectSensors() {
        super.disconnectSensors();
        disconnectServiceUpIsRunning();
    }

    @Override
    public void updateHosts(List<String> mpiHosts) {

        log.info("mpi hosts on MPINodeImpl are {}", mpiHosts.toString());
        getDriver().updateHosts(mpiHosts);
    }


    @Override
    public Boolean isMaster() {
        return (Boolean.TRUE.equals(getAttribute(MPINode.MASTER_FLAG)));
    }

    @Override
    public MPIDriver getDriver() {
        return (MPIDriver) super.getDriver();
    }

    @Override
    public Class<? extends MPIDriver> getDriverInterface() {
        return MPIDriver.class;
    }
}
