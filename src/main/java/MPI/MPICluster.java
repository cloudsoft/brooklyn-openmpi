package MPI;

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

    AttributeSensor<List<String>> HPC_CLUSTER_NODES = Sensors.newSensor(new TypeToken<List<String>>() {
    },
            "hpccluster.nodes", "List of hosts of all active Open MPI nodes in the cluster (public hostname/IP)");



}
