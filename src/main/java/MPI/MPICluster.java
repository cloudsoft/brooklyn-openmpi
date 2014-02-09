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
import java.util.Map;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */

@ImplementedBy(MPIClusterImpl.class)
public interface MPICluster extends DynamicCluster {

    /*
     * FIXME:
     *  1. If master fails to start, then all slaves will wait forever
     *  2. If master subsequently dies, then new master will not be promoted.
     *     Requires installing private-ssh-key on other nodes.
     *  3. Need to pass mpi_hosts in to effector call on mpiNode for execution
     *     (not implemented yet).
     */

    AttributeSensor<Map<Entity,String>> MPI_CLUSTER_NODES = Sensors.newSensor(new TypeToken<Map<Entity,String>>(){},"mpicluster.nodes", "Hostnames of all active Open MPI nodes in the cluster (public hostname/IP)");
    public static final AttributeSensor<Boolean> MASTER_SSH_KEY_GENERATED = Sensors.newBooleanSensor("mpicluster.master_ssh_key_generated","senses if the master node SSH key has been configured");
    public static final AttributeSensor<MPINode> MASTER_NODE = Sensors.newSensor(MPINode.class,"mpicluster.masternode","the master node for the cluster");

    MethodEffector<Void> RUN_RAY_TRACING = new MethodEffector<Void>(MPICluster.class,"runRayTracingDemo");

    AttributeSensor<Boolean> RAY_TRACING_DEMO_INSTALLED = Sensors.newBooleanSensor("mpiccluster.raytracing.demo.installed","flag to indicate if the ray tracing demo has been installed");
    @Effector(description = "Installs and runs a ray tracing app on my Open-MPI cluster")
    void runRayTracingDemo(@EffectorParam(name="numOfNodes", description="the number of nodes I want to run the demo.") Integer numOfNodes);
}
