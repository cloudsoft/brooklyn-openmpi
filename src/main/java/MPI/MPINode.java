package MPI;


import brooklyn.config.ConfigKey;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.basic.VanillaSoftwareProcess;
import brooklyn.entity.effector.Effectors;
import brooklyn.entity.java.UsesJmx;
import brooklyn.entity.java.VanillaJavaApp;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import brooklyn.event.basic.Sensors;
import brooklyn.util.flags.SetFromFlag;
import com.google.common.reflect.TypeToken;

import java.util.List;

@ImplementedBy(MPINodeImpl.class)
public interface MPINode extends VanillaSoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class, "mpinode.masterFlag", "indicates whether this node is master", Boolean.FALSE);
    ConfigKey<MPINode> MPI_MASTER = ConfigKeys.newConfigKey(MPINode.class, "mpi.master.node");
    AttributeSensor<String> MASTER_PUBLIC_SSH_KEY = Sensors.newStringSensor("mpi.master.publicSshKey");
    AttributeSensor<List<String>> MPI_HOSTS = Sensors.newSensor(new TypeToken<List<String>>() {
    }, "mpinode.hosts", "Hostnames of all active Open MPI nodes in the cluster (public hostname/IP)");
    AttributeSensor<Boolean> ALL_HOSTS_UP = Sensors.newBooleanSensor("mpinode.all.hosts.up");



    //public static final MethodEffector<Void> UPDATE_HOSTS = new MethodEffector<Void>(MPINode.class,"updateHosts");


    AttributeSensor<Boolean> GE_INSTALLED = Sensors.newBooleanSensor("mpinode.sgeInstalled", "flag to indicate if Grid Engine was installed");
    AttributeSensor<Integer> NUM_OF_PROCESSORS = Sensors.newIntegerSensor("mpinode.num_of_processors", "attribute that shows the number of proceesors");

    @SetFromFlag("SGEConfigTemplate")
    ConfigKey<String> SGE_CONFIG_TEMPLATE_URL = ConfigKeys.newStringConfigKey(
            "mpicluster.sgeconfig.template", "Template file (in freemarker format) for configuring the SGE installation",
            "classpath://gridengineinstalltemplate");

    @Effector(description = "updates the hosts list for the node")
    public void updateHosts(@EffectorParam(name = "mpiHosts", description = "list of all mpi hosts in the cluster") List<String> mpiHosts);

    public Boolean isMaster();


}
