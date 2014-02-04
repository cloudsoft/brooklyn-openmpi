import MPI.MPICluster;
import MPI.MPINode;
import brooklyn.entity.basic.AbstractApplication;
import brooklyn.entity.proxying.EntitySpec;

public class MPIExample extends AbstractApplication {

    public void init()
    {
        addChild(EntitySpec.create(MPICluster.class)
                .configure(MPICluster.INITIAL_SIZE,2)
                .configure(MPICluster.MEMBER_SPEC, EntitySpec.create(MPINode.class)));
    }
}