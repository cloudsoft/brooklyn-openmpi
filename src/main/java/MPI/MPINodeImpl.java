package MPI;

import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.location.Location;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPINodeImpl extends SoftwareProcessImpl implements MPINode {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPINodeImpl.class);
    private AtomicBoolean setNoOfProcessors;

    @Override
    public void init() {
        super.init();

    }

    @Override
    protected void doStart(Collection<? extends Location> locations) {
        super.doStart(locations);
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

        if (getDriver() == null)
            log.info("driver is null!");


        log.info("my driver is {}", getDriver().toString());

        try {
            getDriver().updateHosts(mpiHosts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public Boolean isMaster() {
        return (Boolean.TRUE.equals(getAttribute(MPINode.MASTER_FLAG)));
    }


    @Override
    public Class<? extends MPIDriver> getDriverInterface() {
        return MPIDriver.class;
    }

    @Override
    public MPIDriver getDriver() {
        return (MPIDriver) super.getDriver();
    }

}
