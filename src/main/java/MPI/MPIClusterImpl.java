package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.trait.Startable;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.Location;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.ssh.BashCommands;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static brooklyn.util.JavaGroovyEquivalents.groovyTruth;

public class MPIClusterImpl extends DynamicClusterImpl implements MPICluster {

    private static final Logger log = LoggerFactory.getLogger(MPIClusterImpl.class);
    private AtomicBoolean masterSshgenerated = new AtomicBoolean();
    private AtomicInteger memberTrackerCount = new AtomicInteger(0);

    public void init() {
        log.info("Initializing the Open-MPI Cluster.");
        super.init();

        memberTrackerCount.set(getConfig(INITIAL_SIZE));


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

                log.info("Setting master node to be {}", member.getId());

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

                log.info("Added new MPI member to {}: {}; {}", new Object[]{this, member, address});

                nodes.put(member, address);
                setAttribute(MPI_CLUSTER_NODES, nodes);

            }
        } else {
            Map<Entity, String> nodes = getAttribute(MPI_CLUSTER_NODES);
            if (nodes != null) {
                String address = nodes.remove(member);
                setAttribute(MPI_CLUSTER_NODES, nodes);
                log.info("Removed MPI member from {}: {}; {}", new Object[]{this, member, address});

                log.info("Updating mpi_hosts to all members");

                MPINode masterNode = getAttribute(MPICluster.MASTER_NODE);

                Entities.invokeEffectorWithArgs(this, masterNode, MPINode.UPDATE_HOSTS, Optional.of(Lists.newArrayList(nodes.values())).get());
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

    protected void connectSensors() {

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
        subscribeToMembers(this, MPINode.MASTER_PUBLIC_SSH_KEY, new SensorEventListener<Object>() {
            @Override
            public void onEvent(SensorEvent<Object> event) {
                if (event.getValue() != null && !masterSshgenerated.getAndSet(true)) {

                    log.info("Master public ssh key has been generated...");
                    for (MPINode node : Iterables.filter(getMembers(), MPINode.class)) {
                        ((EntityInternal) node).setAttribute(MPINode.MASTER_PUBLIC_SSH_KEY, (String) event.getValue());
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

    @Override
    public void runRayTracingDemo(Integer numOfNodes) {


        numOfNodes = Optional.fromNullable(numOfNodes).or(1);
        if (numOfNodes <= 0 && !(numOfNodes instanceof Integer))
            throw new IllegalArgumentException(String.format("Illegal number of processes: %s", numOfNodes));
        //install the demo on all machines
        if (getAttribute(MPICluster.RAY_TRACING_DEMO_INSTALLED) == null || Boolean.FALSE.equals(getAttribute(MPICluster.RAY_TRACING_DEMO_INSTALLED))) {

            //get all children's ssh machines
            List<SshMachineLocation> myLocs = Lists.newArrayList(Iterables.transform(Iterables.filter(getMembers(), Predicates.instanceOf(MPINode.class)), new Function<Entity, SshMachineLocation>() {

                @Nullable
                @Override
                public SshMachineLocation apply(@Nullable Entity entity) {
                    return (SshMachineLocation) Iterables.find(entity.getLocations(), Predicates.instanceOf(SshMachineLocation.class));
                }
            }));


            for (SshMachineLocation loc : myLocs) {
                loc.execScript("installing Ray Tracing app",
                        ImmutableList.of(BashCommands.commandToDownloadUrlAs(
                                "http://jedi.ks.uiuc.edu/~johns/raytracer/files/0.99b2/tachyon-0.99b2.tar.gz", "tachyon.tar.gz"),
                                "tar xvfz tachyon.tar.gz",
                                "cd tachyon/unix",
                                "make linux-mpi"));

                log.info("installing tachyon ray tracing benchmark on Node: {}", loc.getId());
            }

            setAttribute(MPICluster.RAY_TRACING_DEMO_INSTALLED, true);
        }

        //execute the ray tracing demo on the master node

        MPINode masterNode = getAttribute(MPICluster.MASTER_NODE);
        SshMachineLocation masterLocation = (SshMachineLocation) Iterables.find(masterNode.getLocations(), Predicates.instanceOf(SshMachineLocation.class));

        log.info("running the ray tracing benchmark with {} processes", numOfNodes);
        // TODO run different options for the ray tracing benchmark.
        masterLocation.
                execScript("Executing the demo",
                        ImmutableList.of(String.format("mpirun -np %s --hostfile ~/mpi_hosts ~/tachyon/compile/linux-mpi/tachyon ~/tachyon/scenes/teapot.dat -format BMP -o teapot.%s.bmp > " +
                                "~/raytraceout.%s",
                                numOfNodes, numOfNodes, numOfNodes)));

//        DynamicTasks.queueIfPossible(SshEffectorTasks.ssh(ImmutableList.of(String.format("mpirun -np %s --hostfile ~/mpi_hosts ~/tachyon/compile/linux-mpi/tachyon ~/tachyon/scenes/teapot.dat -format BMP -o teapot.%s.bmp > " +
//                "~/raytraceout.%s",numOfNodes,numOfNodes,numOfNodes)))
//                .machine(masterLocation)
//                .summary("Running the teapot ray tracing benchmark."))
//        .orSubmitAndBlock(masterNode);
        // FIXME display results on console instead of copying a file across.
        log.info("copying results to local machine: {}", "raytraceout." + numOfNodes + "." + masterLocation.getId());
        masterLocation.copyFrom("raytraceout." + numOfNodes, "raytraceout." + numOfNodes + "." + masterLocation.getId());
        log.info("fetching the teapot");
        masterLocation.copyFrom(String.format("teapot.%s.bmp", numOfNodes), "teapot." + numOfNodes + ".bmp");


    }

}
