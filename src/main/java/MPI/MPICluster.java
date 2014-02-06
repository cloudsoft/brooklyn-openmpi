package MPI;

import brooklyn.config.ConfigKey;
import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.Sensors;
import com.google.common.reflect.TypeToken;

import java.util.List;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */

@ImplementedBy(MPIClusterImpl.class)
public interface MPICluster extends DynamicCluster {

    AttributeSensor<List<String>> MPI_CLUSTER_NODES = Sensors.newSensor(new TypeToken<List<String>>(){},"mpicluster.nodes", "List of hosts of all active Open MPI nodes in the cluster (public hostname/IP)");
    public static final AttributeSensor<Boolean> MASTER_SSH_KEY_GENERATED = Sensors.newBooleanSensor("mpicluster.master_ssh_key_generated","senses if the master node SSH key has been configured");
    public static final AttributeSensor<MPINode> MASTER_NODE = Sensors.newSensor(MPINode.class,"mpicluster.masternode","the master node for the cluster");

    MethodEffector<Void> SIMPLE_COMPILE = new MethodEffector<Void>(MPICluster.class,"simpleCompile");
//    MethodEffector<Void> COMPILE_AND_RUN = new MethodEffector<Void>(Void.class,"compileAndRun");

//    @Effector(description="compiles and runs a file from url specified")
//    public void compileAndRun(@EffectorParam(name="fileurl",description="url of .c file") String url);
    @Effector(description = "gets and compiles a file on all MPI nodes")
    void simpleCompile(@EffectorParam(name="fileurl") String url);
}
