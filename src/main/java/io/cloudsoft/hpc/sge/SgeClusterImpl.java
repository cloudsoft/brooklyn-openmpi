package io.cloudsoft.hpc.sge;

import brooklyn.entity.Entity;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static brooklyn.util.JavaGroovyEquivalents.groovyTruth;

public class SgeClusterImpl extends DynamicClusterImpl implements SgeCluster {

    private static final Logger log = LoggerFactory.getLogger(SgeClusterImpl.class);
    private final AtomicBoolean hostsInitialized = new AtomicBoolean();
    private AtomicBoolean masterSshgenerated = new AtomicBoolean();
    private Map<Entity, String> onlineMembers = Maps.newConcurrentMap();
    private volatile Integer totalNumberOfProcessors = 0;

    public void init() {
        log.info("Initializing the SGE Cluster.");
        super.init();

        subscribeToMembers(this, SgeNode.HOSTNAME, new SensorEventListener<String>() {
            @Override
            public void onEvent(SensorEvent<String> stringSensorEvent) {
                if (!hostsInitialized.get()) {
                    if (stringSensorEvent.getValue() != null) {
                        onlineMembers.put(stringSensorEvent.getSource(), stringSensorEvent.getValue());

                        if (onlineMembers.size() == getConfig(INITIAL_SIZE)) {
                            synchronized (SgeClusterImpl.this) {

                                Entities.invokeEffectorListWithArgs(SgeClusterImpl.this, getMembers(), SgeNode.UPDATE_HOSTS, onlineMembers.values());

                            }
                            hostsInitialized.set(true);
                        }
                    }
                }
            }

        });

        subscribeToMembers(this, SgeNode.NUM_OF_PROCESSORS, new SensorEventListener<Integer>() {
            @Override
            public void onEvent(SensorEvent<Integer> integerSensorEvent) {
                if (integerSensorEvent.getValue() != null) {
                    synchronized (SgeClusterImpl.this) {
                        totalNumberOfProcessors += integerSensorEvent.getValue();
                        setAttribute(SgeCluster.TOTAL_NUMBER_OF_PROCESSORS, totalNumberOfProcessors);
                    }

                }
            }
        });


//        subscribeToMembers(this, SgeNode.SERVICE_UP, new SensorEventListener<Boolean>() {
//            @Override
//            public void onEvent(SensorEvent<Boolean> event) {
//                // FIXME comments about why have two subscriptions
//                // FIXME Need to track memberRemoved.
//                //       Could/should extract these add/remove blocks into re-usable methods: onMemberDown/onMemberUp?
//                if (hostsInitialized.get()) {
//                    SgeNode member = (SgeNode) event.getSource();
//                    if (Boolean.TRUE.equals(event.getValue())) {
//                        if (!onlineMembers.containsKey(member)) {
//                            SgeNode master = getMaster();
//                            log.info("adding {} to the SGE Cluster", member.getId());
//                            Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.ADD_SLAVE, member);
//                            Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.UPDATE_PE, "mpi_pe", totalNumberOfProcessors);
//                        }
//                    } else if (Boolean.FALSE.equals(event.getValue())) {
//                        // FIXME Need to ensure concurrent calls from slave.stop() to removeSlave() are idempotent
//
//                        if (onlineMembers.containsKey(member)) {
//                            SgeNode master = getMaster();
//                            log.info("removing {} from the SGE Cluster", member.getId());
//                            Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.REMOVE_SLAVE, member);
//                            Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.UPDATE_PE, "mpi_pe", totalNumberOfProcessors);
//                        }
//                    }
//                }
//            }
//        });
    }

    @Override
    protected Entity createNode(Location loc, Map<?, ?> flags) {
        Entity member = super.createNode(loc, flags);

        // TODO Can we rely on non-concurrent calls to createNode?
        synchronized (this) {
            Entity master = getAttribute(MASTER_NODE);
            if (master == null) {
                ((EntityInternal) member).setConfig(SgeNode.MASTER_FLAG, true);
                master = member;

                log.info("Setting master node to be {}", member.getId());

                setAttribute(MASTER_NODE, (SgeNode) master);
                ((EntityInternal) member).setDisplayName(String.format("Sge Master Node:%s", member.getId()));
            } else {
                ((EntityInternal) member).setConfig(SgeNode.MASTER_FLAG, false);
                ((EntityInternal) member).setDisplayName(String.format("Sge Slave Node:%s", member.getId()));
            }

            // set the master node for each new entity.
            ((EntityInternal) member).setConfig(SgeNode.SGE_MASTER, (SgeNode) master);

        }

        return member;
    }

    protected synchronized void onServerPoolMemberChanged(Entity member) {
        if (log.isTraceEnabled()) log.trace("For {}, considering membership of {} which is in locations {}",
                new Object[]{this, member, member.getLocations()});
        if (belongsInServerPool(member)) {
            Map<Entity, String> nodes = getAttribute(SGI_CLUSTER_NODE);
            if (nodes == null) nodes = Maps.newLinkedHashMap();
            String address = getAddressOfEntity(member);
            if (address == null) {
                log.error("Unable to construct hostname:port representation for {} ({}:{}); skipping in {}");
            } else {
                SgeNode master = getAttribute(SgeCluster.MASTER_NODE);
                log.info("Adding new Sge member to {}: {}; {}", new Object[]{this, member, address});

                nodes.put(member, address);
                setAttribute(SGI_CLUSTER_NODE, nodes);

                Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.ADD_SLAVE, member);
                Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.UPDATE_PE, "mpi_pe", totalNumberOfProcessors);

            }
        } else {
            Map<Entity, String> nodes = getAttribute(SGI_CLUSTER_NODE);
            if (nodes != null) {
                String address = nodes.remove(member);
                setAttribute(SGI_CLUSTER_NODE, nodes);
                log.info("Removing Sge member from {}: {}; {}", new Object[]{this, member, address});

                log.info("Updating sge hosts to all members");

                SgeNode master = getAttribute(MASTER_NODE);

                Entities.invokeEffectorWithArgs(this, master, SgeNode.UPDATE_HOSTS, Optional.of(Lists.newArrayList(nodes.values())).get());

                Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.REMOVE_SLAVE, member);
                Entities.invokeEffectorWithArgs(SgeClusterImpl.this, master, SgeNode.UPDATE_PE, "mpi_pe", totalNumberOfProcessors);
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
        return member.getAttribute(SgeNode.ADDRESS);
    }

    protected void connectSensors() {

        Map<String, Object> flags = MutableMap.<String, Object>builder()
                .put("name", "Controller targets tracker")
                .put("sensorsToTrack", ImmutableSet.of(SgeNode.ADDRESS))
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
        subscribeToMembers(this, SgeNode.MASTER_PUBLIC_SSH_KEY, new SensorEventListener<Object>() {
            @Override
            public void onEvent(SensorEvent<Object> event) {
                if (event.getValue() != null && !masterSshgenerated.getAndSet(true)) {

                    log.info("Master public ssh key has been generated...");
                    for (SgeNode node : Iterables.filter(getMembers(), SgeNode.class)) {
                        ((EntityInternal) node).setAttribute(SgeNode.MASTER_PUBLIC_SSH_KEY, (String) event.getValue());
                    }

                }

            }
        });


    }

    @Override
    public void start(Collection<? extends Location> locations) {

        super.start(locations);

        connectSensors();


    }

    public SgeNode getMaster() {
        return getAttribute(SgeCluster.MASTER_NODE);

    }


}
