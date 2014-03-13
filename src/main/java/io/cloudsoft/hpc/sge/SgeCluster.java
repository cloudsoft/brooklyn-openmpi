package io.cloudsoft.hpc.sge;

import brooklyn.entity.Entity;
import brooklyn.entity.annotation.Effector;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.MethodEffector;
import brooklyn.entity.group.DynamicCluster;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.Sensors;
import com.google.common.reflect.TypeToken;

import java.util.Map;


@ImplementedBy(SgeClusterImpl.class)
public interface SgeCluster extends DynamicCluster {

    AttributeSensor<Map<Entity,String>> SGI_CLUSTER_NODE = Sensors.newSensor(new TypeToken<Map<Entity,String>>(){},"sge.cluster.nodes", "Hostnames of all active SGE nodes in the cluster (public hostname/IP)");
    public static final AttributeSensor<Boolean> MASTER_SSH_KEY_GENERATED = Sensors.newBooleanSensor("sge.cluster.master_ssh_key_generated","senses if the master node SSH key has been configured");
    public static final AttributeSensor<SgeNode> MASTER_NODE = Sensors.newSensor(SgeNode.class,"sge.cluster.masternode","the master node for the cluster");
    AttributeSensor<Integer> TOTAL_NUMBER_OF_PROCESSORS = Sensors.newIntegerSensor("sge.cluster.total.no.of.processors","sensor to track all the number of processors in the pool");



}
