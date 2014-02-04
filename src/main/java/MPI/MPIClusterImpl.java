package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.util.collections.MutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPIClusterImpl extends DynamicClusterImpl implements MPICluster {

    private static final Logger log = LoggerFactory.getLogger(MPIClusterImpl.class);
    private final AtomicBoolean masterSelected = new AtomicBoolean();
    private List<String> hostnamesList = Lists.newArrayList();

    public void init() {
        log.info("Initializing the Open-MPI Cluster.");

        super.init();
        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "YCSB Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {


                if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) {
                    //set the hostnames for the HPC cluster
                    hostnamesList.add(member.getAttribute(MPINode.HOSTNAME));
                    setAttribute(HPC_CLUSTER_NODES, Lists.newArrayList(hostnamesList));

                    //selects first member to be the master node
                    if (!masterSelected.getAndSet(true)) {
                        ((EntityInternal) member).setAttribute(MPINode.MASTER_FLAG, Boolean.TRUE);
                    }

                }
            }

            @Override
            protected void onEntityRemoved(Entity member) {
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

        //
    }

    @Override
    protected EntitySpec<?> getMemberSpec() {
        return getConfig(MEMBER_SPEC, EntitySpec.create(MPINode.class));
    }

    @Override
    public synchronized boolean addMember(Entity member) {
        boolean result = super.addMember(member);
        setAttribute(SERVICE_UP, calculateServiceUp());
        return result;
    }

    @Override
    public synchronized boolean removeMember(Entity member) {
        boolean result = super.removeMember(member);
        setAttribute(SERVICE_UP, calculateServiceUp());
        return result;
    }

    @Override
    protected boolean calculateServiceUp() {
        boolean up = false;
        for (Entity member : getMembers()) {
            if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) up = true;
        }
        return up;
    }
}
