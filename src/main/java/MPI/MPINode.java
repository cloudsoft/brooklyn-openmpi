package MPI;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */

import brooklyn.entity.Effector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class,"hpcnode.master_flag","flags a random node to be the master node",Boolean.FALSE);

    public static final Effector<String> SET_FETCH_MASTER_SSH_ID = Effectors.effector(String.class, "setAndFetchMasterSshKey")
            .description("sets the master SSH Id")
            .buildAbstract();

    public void setAndFetchMasterSshKey();
}
