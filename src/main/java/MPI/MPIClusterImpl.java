package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.annotation.EffectorParam;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.trait.Startable;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.location.Location;
import brooklyn.util.collections.MutableMap;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static brooklyn.util.JavaGroovyEquivalents.groovyTruth;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPIClusterImpl extends DynamicClusterImpl implements MPICluster {

    private static final Logger log = LoggerFactory.getLogger(MPIClusterImpl.class);
    private AtomicBoolean masterSshgenerated = new AtomicBoolean();

    public void init() {
        log.info("Initializing the Open-MPI Cluster.");
        super.init();

        // TODO If choose to set mpi_hosts on each node, then do this:
        //for (MPINode entity : Iterables.filter(MPIClusterImpl.this.getMembers(), MPINode.class)) {
        //    Entities.invokeEffector(MPIClusterImpl.this, entity, MPINode.UPDATE_HOSTS_FILE);
        //}
    }

    @Override
    protected Entity createNode(Location loc, Map<?, ?> flags) {
        Entity member = super.createNode(loc, flags);

        // TODO Can we rely on non-concurrent calls to createNode?
        synchronized (this) {
            Entity master = getAttribute(MPICluster.MASTER_NODE);
            if (master == null) {
                ((EntityInternal) member).setConfig(MPINode.MASTER_FLAG, true);
                master = member;
                setAttribute(MPICluster.MASTER_NODE, (MPINode) master);
            } else {
                ((EntityInternal) member).setConfig(MPINode.MASTER_FLAG, false);
            }

            // set the master node for each new entity.
            ((EntityInternal) member).setConfig(MPINode.MPI_MASTER, (MPINode) master);
        }

        return member;
    }

    protected synchronized void onServerPoolMemberChanged(Entity member) {
        if (log.isTraceEnabled()) log.trace("For {}, considering membership of {} which is in locations {}",
                new Object[]{this, member, member.getLocations()});
        if (belongsInServerPool(member)) {
            Map<Entity, String> nodes = getAttribute(MPI_CLUSTER_NODES);
            if (nodes == null) nodes = Maps.newLinkedHashMap();
            String address = getAddressOfEntity(member);
            if (address == null) {
                log.error("Unable to construct hostname:port representation for {} ({}:{}); skipping in {}");
            } else {
                nodes.put(member, address);
                setAttribute(MPI_CLUSTER_NODES, nodes);

                log.info("Added new MPI member to {}: {}; {}", new Object[]{this, member, address});

                log.info("Updating mpi_hosts to all members");

            for(Entity node: nodes.keySet())
                Entities.invokeEffectorWithArgs(this, node, MPINode.UPDATE_HOSTS_FILE, Optional.of(Lists.newArrayList(nodes.values())).get());


            }
        } else {
            Map<Entity, String> nodes = getAttribute(MPI_CLUSTER_NODES);
            if (nodes != null) {
                String address = nodes.remove(member);
                setAttribute(MPI_CLUSTER_NODES, nodes);
                log.info("Removed MPI member from {}: {}; {}", new Object[]{this, member, address});

                log.info("Updating mpi_hosts to all members");

                for(Entity node: nodes.keySet())
                    Entities.invokeEffectorWithArgs(this, node, MPINode.UPDATE_HOSTS_FILE, Optional.of(Lists.newArrayList(nodes.values())).get());

            }
        }
        if (log.isTraceEnabled()) log.trace("Done {} checkEntity {}", this, member);
    }

    protected boolean belongsInServerPool(Entity member) {
        if (!groovyTruth(member.getAttribute(Startable.SERVICE_UP))) {
            if (log.isTraceEnabled()) log.trace("Members of {}, checking {}, eliminating because not up", this, member);
            return false;
        }
        if (!getMembers().contains(member)) {
            if (log.isTraceEnabled())
                log.trace("Members of {}, checking {}, eliminating because not member", this, member);
            return false;
        }
        if (log.isTraceEnabled()) log.trace("Members of {}, checking {}, approving", this, member);
        return true;
    }

    protected String getAddressOfEntity(Entity member) {
        return member.getAttribute(MPINode.ADDRESS);
    }

    @Override
    public void simpleCompile(@EffectorParam(name = "url") String url) {

        for (MPINode member : Iterables.filter(getChildren(), MPINode.class))
            Entities.invokeEffectorWithArgs(this, member, MPINode.SIMPLE_COMPILE, url);
    }

    protected void connectSensors() {
//        Map<?, ?> policyFlags = MutableMap.of("name", "Controller targets tracker",
//                "sensorsToTrack", ImmutableSet.of(MPINode.HOSTNAME));

        Map<String, Object> flags = MutableMap.<String, Object>builder()
                .put("name", "Controller targets tracker")
                .put("sensorsToTrack", ImmutableSet.of(MPINode.ADDRESS))
                .build();

        AbstractMembershipTrackingPolicy serverPoolMemberTrackerPolicy = new AbstractMembershipTrackingPolicy(flags) {
            protected void onEntityChange(Entity member) {
                onServerPoolMemberChanged(member);
            }

            protected void onEntityAdded(Entity member) {
                onServerPoolMemberChanged(member);
            }

            protected void onEntityRemoved(Entity member) {
                onServerPoolMemberChanged(member);
            }
        };

        addPolicy(serverPoolMemberTrackerPolicy);
        serverPoolMemberTrackerPolicy.setGroup(this);

        //set the public ssh key attribute if it is set by master
        subscribeToMembers(this, MPINode.MASTER_PUBLIC_SSH_KEY,new SensorEventListener<Object>() {
            @Override
            public void onEvent(SensorEvent<Object> event) {
                if (event.getValue() != null && !masterSshgenerated.getAndSet(true))
                {

                    log.info("Master public ssh key has been generated...");
                    for (MPINode node : Iterables.filter(getMembers(),MPINode.class))
                    {
                        ((EntityInternal) node).setAttribute(MPINode.MASTER_PUBLIC_SSH_KEY,(String)event.getValue());
                    }

                }

            }
        });
    }

    public void start(Collection<? extends Location> locations) {

        super.start(locations);

        connectSensors();
    }

}
