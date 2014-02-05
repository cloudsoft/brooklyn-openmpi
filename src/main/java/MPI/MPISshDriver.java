package MPI;

import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
import brooklyn.util.text.Strings;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.codehaus.groovy.tools.shell.util.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPISshDriver extends VanillaSoftwareProcessSshDriver implements MPIDriver {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPISshDriver.class);

    public MPISshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public void install() {
        super.install();

        newScript(INSTALLING)
                .body.append("sudo apt-get update")
                .body.append("sudo apt-get -y install build-essential")
                .body.append("sudo apt-get -y install openmpi-bin openmpi-checkpoint openmpi-common openmpi-doc libopenmpi-dev")
                .execute();
    }

    @Override
    public void customize() {
        //super.customize();
    }

    @Override
    public Map<String, String> getShellEnvironment() {
        return super.getShellEnvironment();
    }

    @Override
    public String getPidFile() {
        return super.getPidFile();
    }

    @Override
    public void launch() {
        //super.launch();


        entity.setConfig(MPINode.MPI_MASTER,DependentConfiguration.attributeWhenReady(getEntity().getParent(), MPICluster.MASTER_NODE));

        //if entity is master..
        if (Boolean.TRUE.equals(entity.getAttribute(MPINode.MASTER_FLAG)))
        {
            log.info("Entity: {} is master now generateing Ssh key", entity.getId());
            setMasterSshKey();

            ((EntityInternal) getEntity().getParent()).setAttribute(MPICluster.MASTER_SSH_KEY_GENERATED, true);

            updateHostsFile();

        }

        else
        {

            //wait for master ssh key to be set before executing.

            entity.setConfig(MPINode.MASTER_KEY_GENERATED,DependentConfiguration.attributeWhenReady(getEntity().getParent(),MPICluster.MASTER_SSH_KEY_GENERATED));
            log.info("Entity: {} is not master copying Ssh key from master {}", entity.getId(),entity.getParent().getAttribute(MPICluster.MASTER_NODE).getId());

            setSshKeyFromMaster();
            updateHostsFile();
        }

    }

    @Override
    public boolean isRunning() {

        entity.setAttribute(MPINode.SERVICE_UP,Boolean.TRUE);
        return true;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void kill() {
        super.kill();
    }

    @Override
    public void updateHostsFile()
    {
        log.info("updateHostsFile invoked");

        getMachine().copyTo(getMPIHostsToCopy(), "mpi_hosts");

    }

    private InputStream getMPIHostsToCopy()
    {

        if (getEntity().getAttribute(MPINode.MPI_HOSTS) != null )
        {
            List<String> mpiHosts = getEntity().getAttribute(MPINode.MPI_HOSTS);
            return Streams.newInputStreamWithContents(Strings.join(mpiHosts,"\n"));
        }
        else
            return Streams.newInputStreamWithContents("");

        //return new ByteArrayInputStream(str.getBytes(Charset.forName("UTF-8")));
    }

    private void setSshKeyFromMaster()
    {
        MPINode master = getEntity().getParent().getAttribute(MPICluster.MASTER_NODE);
        SshMachineLocation master_machine = (SshMachineLocation) Iterables.find(master.getLocations(), Predicates.instanceOf(SshMachineLocation.class));

        log.info("Copying authorized_keys from master {} to slave {}", master.getId(), entity.getId());

        //copy rsa files across from master to local
        master_machine.copyFrom("auth_keys.txt", "pubauthkeys.txt");
        master_machine.copyFrom("id.txt", "pubauthid.txt");

        //send files from local to slave
        getMachine().copyTo(new File("pubauthkeys.txt"), "id_rsa.pub");
        master_machine.copyTo(new File("pubauthid.txt"), "id_rsa");

        getMachine().execCommands("remove strict hosting", ImmutableList.of(
                "chmod 700 ~/.ssh",
                BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config")   ,
                "cat id_rsa >> ~/.ssh/id_rsa",
                "cat id_rsa.pub >> ~/.ssh/id_rsa.pub",
                "cat id_rsa.pub >> ~/.ssh/authorized_keys",
                "chmod 600 ~/.ssh/authorized_keys"));
    }

    private void setMasterSshKey() {

        log.info("generating the master node ssh key");
        entity.setConfig(MPINode.MASTER_KEY_GENERATED,true);
        getMachine().execCommands("setMasterSshKey",
                ImmutableList.of(
                        "chmod 700 ~/.ssh",
                        BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"),
                        "ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"",
                        "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys",
                        "chmod 600 ~/.ssh/authorized_keys",
                        "cp ~/.ssh/id_rsa.pub auth_keys.txt",
                        "cp ~/.ssh/id_rsa id.txt"));


    }
}
