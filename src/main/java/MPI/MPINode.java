package MPI;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */


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
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.List;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class, "mpinode.masterFlag", "indicates whether this node is master", Boolean.FALSE);
    ConfigKey<MPINode> MPI_MASTER = ConfigKeys.newConfigKey(MPINode.class, "mpi.master.node");
    AttributeSensor<String> MASTER_PUBLIC_SSH_KEY = Sensors.newStringSensor("mpi.master.publicSshKey");
    AttributeSensor<List<String>> MPI_HOSTS = Sensors.newSensor(new TypeToken<List<String>>() {}, "mpinode.mpihosts", "A list of all mpi hosts in the cluster");
    //    public void setMasterSshKey();
//    public void copyMasterSshKeyToSlaves(List<String> hostnames);
    MethodEffector<Void> UPDATE_HOSTS_FILE = new MethodEffector<Void>(MPINode.class, "updateHostsFile");
    MethodEffector<Void> SIMPLE_COMPILE = new MethodEffector<Void>(MPINode.class, "simpleCompile");



    @Effector(description = "update the mpi_hosts file")
    public void updateHostsFile(@EffectorParam(name = "mpihosts", description="list of mpi_hosts") List<String> mpiHosts);

//    @Effector(description = "sends a file")
//    public void sendFile(String url);

    @Effector(description = "fetch and compile a file on mpi node")
    void simpleCompile(@EffectorParam(name = "fileurl") String url);

}
