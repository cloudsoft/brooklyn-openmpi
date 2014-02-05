package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.util.collections.MutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPIClusterImpl extends DynamicClusterImpl implements MPICluster {

    private static final Logger log = LoggerFactory.getLogger(MPIClusterImpl.class);
    private final Map<Entity, Boolean> membersUpness = Maps.newConcurrentMap();
    private final AtomicBoolean masterSelected = new AtomicBoolean();
    private final AtomicBoolean masterSshKeyGenerated = new AtomicBoolean();
    //private final AtomicBoolean masterSshGenerated = new AtomicBoolean();
    private final Set<String> hostnamesSet = Sets.newHashSet();
    private MPINode masterNode = null;

    public void init() {
        log.info("Initializing the Open-MPI Cluster.");

        super.init();


        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "MPI Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {


                if (!masterSelected.getAndSet(true)) {
                    setAttribute(MPICluster.MASTER_NODE, (MPINode) member);
                    ((EntityInternal) member).setAttribute(MPINode.MASTER_FLAG, true);
                }
                if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) {
                    //set the hostnames for the HPC cluster.
                    log.info("adding new hostname: " + member.getAttribute(MPINode.HOSTNAME));

                    hostnamesSet.add(member.getAttribute(MPINode.HOSTNAME));
                    setAttribute(MPI_CLUSTER_NODES, Lists.newArrayList(hostnamesSet));

                    //sets all available hosts to the member and update the mpi_hosts file.

                    log.info("invoking update mpi_hosts to member " + member.getId());



                    //update the mpi_hosts file
                    for (MPINode entity : Iterables.filter(MPIClusterImpl.this.getMembers(), MPINode.class))
                    {
                        ((EntityInternal) entity).setAttribute(MPINode.MPI_HOSTS, Lists.newArrayList(hostnamesSet));
                        Entities.invokeEffector(MPIClusterImpl.this, entity, MPINode.UPDATE_HOSTS_FILE);
                    }

                }
            }

            @Override
            protected void onEntityRemoved(Entity member) {

                hostnamesSet.remove(member.getAttribute(MPINode.HOSTNAME));
                setAttribute(MPI_CLUSTER_NODES, Lists.newArrayList(hostnamesSet));

                log.info("invoking update mpi_hosts to member " + member.getId());

                for (MPINode entity : Iterables.filter(MPIClusterImpl.this.getMembers(), MPINode.class))
                {
                    ((EntityInternal) entity).setAttribute(MPINode.MPI_HOSTS, Lists.newArrayList(hostnamesSet));
                    Entities.invokeEffector(MPIClusterImpl.this, entity, MPINode.UPDATE_HOSTS_FILE);
                }
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

        subscribeToChildren(this, Attributes.HOSTNAME,new SensorEventListener<String>() {
            @Override
            public void onEvent(SensorEvent<String> event) {
                if (!event.getValue().equals(null))
                {
                    hostnamesSet.add(event.getValue());
                    for (MPINode entity : Iterables.filter(MPIClusterImpl.this.getMembers(), MPINode.class))
                        Entities.invokeEffector(MPIClusterImpl.this, entity, MPINode.UPDATE_HOSTS_FILE);
                }
            }
        });

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

    @Override
    public Integer resize(Integer desiredSize) {
        return super.resize(desiredSize);
    }

    //compiles and runs a c/c++ program through Open MPI
//    @Override
//    public void compileAndRun(@EffectorParam(name = "fileurl", description = "url of .c file") String url) {
//
//
//    }
}
