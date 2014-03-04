package io.cloudsoft.hpc.sge;


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
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;

@ImplementedBy(SgeNodeImpl.class)
public interface SgeNode extends SoftwareProcess {

    BasicAttributeSensorAndConfigKey<Boolean> MASTER_FLAG = new BasicAttributeSensorAndConfigKey<Boolean>(Boolean.class, "mpinode.masterFlag", "indicates whether this node is master", Boolean.FALSE);
    ConfigKey<SgeNode> SGE_MASTER = ConfigKeys.newConfigKey(SgeNode.class, "mpi.master.node");
    AttributeSensor<String> MASTER_PUBLIC_SSH_KEY = Sensors.newStringSensor("mpi.master.publicSshKey");
    AttributeSensor<List<String>> SGE_HOSTS = Sensors.newSensor(new TypeToken<List<String>>() {
    }, "mpinode.hosts", "Hostnames of all active Open MPI nodes in the cluster (public hostname/IP)");
    AttributeSensor<Boolean> ALL_HOSTS_UP = Sensors.newBooleanSensor("mpinode.all.hosts.up");


    ConfigKey<String> MPI_VERSION = ConfigKeys.newStringConfigKey("mpi.version", "version of MPI", "1.6.5");

    @SetFromFlag("downloadUrl")
    BasicAttributeSensorAndConfigKey<String> DOWNLOAD_URL = new BasicAttributeSensorAndConfigKey<String>(
            SoftwareProcess.DOWNLOAD_URL, "thing");


    @SetFromFlag("downloadAddonUrls")
    BasicAttributeSensorAndConfigKey<Map<String,String>> DOWNLOAD_ADDON_URLS = new BasicAttributeSensorAndConfigKey<Map<String,String>>(
            SoftwareProcess.DOWNLOAD_ADDON_URLS, ImmutableMap.of(
            "mpi", "http://developers.cloudsoftcorp.com/brooklyn/repository/io-mpi/${addonversion}/openmpi-${addonversion}-withsge.tar.gz"));



    public static final MethodEffector<Void> UPDATE_HOSTS = new MethodEffector<Void>(SgeNode.class,"updateHosts");


    AttributeSensor<Boolean> GE_INSTALLED = Sensors.newBooleanSensor("mpinode.sgeInstalled", "flag to indicate if Grid Engine was installed");
    AttributeSensor<Integer> NUM_OF_PROCESSORS = Sensors.newIntegerSensor("mpinode.num_of_processors", "attribute that shows the number of proceesors");

    @SetFromFlag("SGEConfigTemplate")
    ConfigKey<String> SGE_CONFIG_TEMPLATE_URL = ConfigKeys.newStringConfigKey(
            "mpicluster.sgeconfig.template", "Template file (in freemarker format) for configuring the io installation",
            "classpath://sge_installation");

    @Effector(description = "updates the hosts list for the node")
    public void updateHosts(@EffectorParam(name = "mpiHosts", description = "list of all mpi hosts in the cluster") List<String> mpiHosts);

    public Boolean isMaster();

    @Effector(description="Run on the master, will remove the slave-node's jobs")
    public void removeSlave(SgeNode slave);

    @Effector(description="Run on the master, will add the slave-node so jobs can be executed on it")
    public void addSlave(SgeNode slave);
}
