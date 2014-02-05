package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.group.AbstractMembershipTrackingPolicy;
import brooklyn.entity.group.DynamicClusterImpl;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.event.AttributeSensor;
import brooklyn.event.SensorEvent;
import brooklyn.event.SensorEventListener;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.event.basic.Sensors;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.ssh.BashCommands;
import com.google.common.collect.*;
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


        // wait until master up

        // wait until all initial nodes up

        AbstractMembershipTrackingPolicy policy = new AbstractMembershipTrackingPolicy(MutableMap.of("name", "MPI Entities")) {
            @Override
            protected void onEntityChange(Entity member) {
            }

            @Override
            protected void onEntityAdded(Entity member) {

                //choose a master node
                if (!masterSelected.get())
                {
                    masterNode = (MPINode) member;
                    ((EntityInternal) member).setAttribute(MPINode.MASTER_FLAG, Boolean.TRUE);

                    //wait for master node to start
                    DependentConfiguration.attributeWhenReady(member,Attributes.SERVICE_UP);

                    //generate the master Ssh key.
                    setMasterSshKey();
                    setAttribute(MPICluster.MASTER_NODE,(MPINode)member);
                    masterSelected.set(true);
                }

                if (Boolean.TRUE.equals(member.getAttribute(SERVICE_UP))) {
                    //set the hostnames for the HPC cluster.
                    log.info("adding new hostname: " + member.getAttribute(MPINode.HOSTNAME));

                    hostnamesSet.add(member.getAttribute(MPINode.HOSTNAME));
                    setAttribute(MPI_CLUSTER_NODES, Lists.newArrayList(hostnamesSet));

                    //sets all available hosts to the member and update the mpi_hosts file.

                    log.info("invoking update mpi_hosts to member " + member.getId());
                    ((EntityInternal) member).setAttribute(MPINode.MPI_HOSTS, Lists.newArrayList(hostnamesSet));
                    Entities.invokeEffector(MPIClusterImpl.this, member, MPINode.UPDATE_HOSTS_FILE);


                }
            }

            @Override
            protected void onEntityRemoved(Entity member) {

                hostnamesSet.remove(member.getAttribute(MPINode.HOSTNAME));

                //update the mpi_hosts file
                for (MPINode entity : Iterables.filter(getMembers(), MPINode.class))
                    Entities.invokeEffector(MPIClusterImpl.this, entity, MPINode.UPDATE_HOSTS_FILE);
            }
        };
        addPolicy(policy);
        policy.setGroup(this);

//        subscribeToMembers(this, Attributes.SERVICE_UP, new SensorEventListener<Boolean>() {
//
//
//            @Override
//            public void onEvent(SensorEvent<Boolean> event) {
//                if (Boolean.TRUE.equals(event.getValue())) {
//                    Entity myEntity = event.getSource();
//                    if (myEntity instanceof MPINode) {
//                        membersUpness.put(myEntity, event.getValue());
//                    }
//
//                }
//
//            }
//
//        });
//
//        subscribe(this,Attributes.SERVICE_UP,new SensorEventListener<Boolean>() {
//            @Override
//            //assuming all nodes are up we set up the master node SSH key
//            public void onEvent(SensorEvent<Boolean> event) {
//                if (Boolean.TRUE.equals(event.getValue()) && !masterSshKeyGenerated.get())
//                {
//                    setMasterSshKey();
//                    masterSshKeyGenerated.set(true);
//                    setAttribute(MPICluster.MASTER_SSH_KEY_GENERATED,true);
//                }
//            }
//        });



    }

    public void setMasterSshKey() {

        log.info("set Master Ssh key invoked");
        SshMachineLocation loc = (SshMachineLocation) Iterables.filter(masterNode.getLocations(), SshMachineLocation.class);

        loc.execCommands("setMasterSshKey",
                ImmutableList.of(
                "chmod 700 ~/.ssh",
                BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"),
                "ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"",
                "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
                "chmod 600 ~/.ssh/authorized_keys "));


    }

    //copy the ssh keys from master to slaves
//    @Override
//    public void copyMasterSshKeyToSlaves(List<String> hostnames)
//    {
//        log.info("copy master ssh to slaves invoked");
//            List<String> cmds = Lists.newArrayList();
//
//            for (String host: hostnames)
//            {
//                getDriver().getMachine().
//                log.info(String.format("ssh-copy-id %s@%s", getUser(), host));
//                cmds.add(String.format("ssh-copy-id %s@%s", getMachine().getUser(), host));
//            }
//
//            newScript(LAUNCHING)
//                    .body.append(cmds)
//                    .execute();
//
//    }

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
