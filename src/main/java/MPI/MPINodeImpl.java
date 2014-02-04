package MPI;

import brooklyn.entity.basic.SoftwareProcessImpl;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPINodeImpl extends SoftwareProcessImpl implements MPINode {
    @Override
    public Class getDriverInterface() {
        return MPIDriver.class;
    }

    @Override
    public void init()
    {
        super.init();
    }

    @Override
    public void setAndFetchMasterSshKey() {
        ((MPIDriver)getDriver()).setAndFetchMasterSshKey();
    }
}
