package MPI;

import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import com.google.common.base.Predicates;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPINodeImpl extends SoftwareProcessImpl implements MPINode {

    private AtomicBoolean setNoOfProcessors;
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPINodeImpl.class);
    @Override
    public Class getDriverInterface() {
        return MPIDriver.class;
    }

    @Override
    public void init() {
        super.init();

    }


    @Override
    protected void connectSensors() {
        super.connectSensors();
        connectServiceUpIsRunning();

    }

    @Override
    protected void disconnectSensors() {
        super.disconnectSensors();
        disconnectServiceUpIsRunning();
    }

    @Override
    public void updateHostsFile(List<String> mpiHosts) {
        ((MPIDriver) getDriver()).updateHostsFile(mpiHosts);
    }


    @Override
    public Boolean isMaster() {
        return (Boolean.TRUE.equals(getAttribute(MPINode.MASTER_FLAG)));
    }



}
