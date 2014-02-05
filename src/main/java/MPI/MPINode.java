package MPI;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */


import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import brooklyn.event.basic.Sensors;
import com.google.common.reflect.TypeToken;

import java.util.List;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class,"mpinode.master_flag","flags a random node to be the master node",Boolean.FALSE);

    ConfigKey<MPINode> MPI_MASTER = ConfigKeys.newConfigKey(MPINode.class,"mpi.master.node");

    AttributeSensor<List<String>> MPI_HOSTS = Sensors.newSensor(new TypeToken<List<String>>(){},"mpinode.mpihosts","A list of all mpi hosts in the cluster");
//    public void setMasterSshKey();
//    public void copyMasterSshKeyToSlaves(List<String> hostnames);

    MethodEffector<Void> UPDATE_HOSTS_FILE = new MethodEffector<Void>(MPINode.class,"updateHostsFile");

    @Effector(description = "update the mpi_hosts file")
    public void updateHostsFile();
}
