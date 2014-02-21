package MPI;


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
import brooklyn.util.flags.SetFromFlag;

import java.util.List;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class, "mpinode.masterFlag", "indicates whether this node is master", Boolean.FALSE);
    ConfigKey<MPINode> MPI_MASTER = ConfigKeys.newConfigKey(MPINode.class, "mpi.master.node");
    AttributeSensor<String> MASTER_PUBLIC_SSH_KEY = Sensors.newStringSensor("mpi.master.publicSshKey");
    AttributeSensor<String> MPI_HOSTS = Sensors.newStringSensor("mpinode.mpihosts", "A list of all mpi hosts in the cluster");
    MethodEffector<Void> UPDATE_HOSTS_FILE = new MethodEffector<Void>(MPINode.class, "updateHostsFile");


    AttributeSensor<Boolean> GE_INSTALLED = Sensors.newBooleanSensor("mpinode.sgeInstalled", "flag to indicate if Grid Engine was installed");
    AttributeSensor<Integer> NUM_OF_PROCESSORS = Sensors.newIntegerSensor("mpinode.num_of_processors", "attribute that shows the number of proceesors");

    @SetFromFlag("SGEConfigTemplate")
    ConfigKey<String> SGE_CONFIG_TEMPLATE_URL = ConfigKeys.newStringConfigKey(
            "mpicluster.sgeconfig.template", "Template file (in freemarker format) for configuring the SGE installation",
            "classpath://gridengineinstalltemplate");

    @Effector(description = "update the mpi_hosts file")
    public void updateHostsFile(@EffectorParam(name = "mpihosts", description = "list of mpi_hosts") List<String> mpiHosts);

    public Boolean isMaster();



}
