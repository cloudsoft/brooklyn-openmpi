package io.cloudsoft.hpc.mpi;


import brooklyn.config.ConfigKey;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import brooklyn.event.basic.Sensors;
import com.google.common.reflect.TypeToken;

import java.util.List;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class, "mpinode.masterFlag", "indicates whether this node is master", Boolean.FALSE);

    // FIXME it is suggested that OPEN-io.cloudsoft.hpc.mpi better off not ristricting the tcp range (http://www.open-mpi.org/community/lists/users/2006/04/1106.php)
    //ConfigKey<String> TCP_PORTS_RANGE = ConfigKeys.newStringConfigKey("mpi.node.tcp.ports.range","range of ports to be used by the mpi processes e.g. 3000-3300");
    ConfigKey<MPINode> MPI_MASTER = ConfigKeys.newConfigKey(MPINode.class, "mpi.node.master.node");
    AttributeSensor<String> MASTER_PUBLIC_SSH_KEY = Sensors.newStringSensor("mpi.node.master.publicSshKey");
    AttributeSensor<List<String>> MPI_HOSTS = Sensors.newSensor(new TypeToken<List<String>>() {}, "mpi.node.mpihosts", "A list of all MPI hosts in the cluster");
    MethodEffector<Void> UPDATE_HOSTS_FILE = new MethodEffector<Void>(MPINode.class, "updateHostsFile");



    @Effector(description = "update the mpi_hosts file")
    public void updateHostsFile(@EffectorParam(name = "mpihosts", description="list of mpi_hosts") List<String> mpiHosts);



}
