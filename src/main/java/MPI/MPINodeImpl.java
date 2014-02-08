package MPI;

import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.SoftwareProcessImpl;

import java.util.List;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPINodeImpl extends SoftwareProcessImpl implements MPINode {
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
        ((MPIDriver)getDriver()).updateHostsFile(mpiHosts);
    }

    @Override
    public void simpleCompile(String url) {
        ((MPIDriver)getDriver()).simpleCompile(url);
    }
}
