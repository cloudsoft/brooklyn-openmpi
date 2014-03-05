package io.cloudsoft.hpc.sge;

import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.location.Location;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SgeNodeImpl extends SoftwareProcessImpl implements SgeNode {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SgeNodeImpl.class);
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
    protected void doStop() {
        SgeNode master = getConfig(SGE_MASTER);
        master.removeSlave((SgeNode)getProxy());
        super.doStop();
    }

    @Override
    public void removeSlave(SgeNode slave) {
        getDriver().removeSlave(slave.getAttribute(HOSTNAME));
    }

    @Override
    public void addSlave(SgeNode slave) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClusterName() {
        return getConfig(SgeNode.CLUSTER_NAME);
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

        log.info("mpi hosts on SgeNodeImpl are {}", mpiHosts.toString());

        setAttribute(SGE_HOSTS,mpiHosts);

    }


    @Override
    public Boolean isMaster() {
        return (Boolean.TRUE.equals(getAttribute(MASTER_FLAG)));
    }


    @Override
    public Class<? extends SgeDriver> getDriverInterface() {
        return SgeDriver.class;
    }

    @Override
    public SgeDriver getDriver() {
        return (SgeDriver) super.getDriver();
    }

}
