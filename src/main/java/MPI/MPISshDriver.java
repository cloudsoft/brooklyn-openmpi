package MPI;

import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
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

        if (Boolean.FALSE.equals(entity.getAttribute(MPINode.MASTER_FLAG)))
        {
            log.info("Entity: " + entity.getId() + " is not master copying Ssh key from master");
            entity.setConfig(MPINode.MPI_MASTER,DependentConfiguration.attributeWhenReady(getEntity().getParent(), MPICluster.MASTER_NODE));
            setSshKeyFromMaster();
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

        getMachine().copyTo(getMPIHostsToCopy(), getRunDir() + "/mpi_hosts");

    }

    private InputStream getMPIHostsToCopy()
    {
       
        List<String> mpiHosts = getEntity().getAttribute(MPINode.MPI_HOSTS);
        StringBuilder strB = new StringBuilder();

        for (String host : mpiHosts)
        {
            strB.append(host + "\n");
        }

        String str = strB.toString();

        return Streams.newInputStreamWithContents(str);
        //return new ByteArrayInputStream(str.getBytes(Charset.forName("UTF-8")));
    }

    private void setSshKeyFromMaster()
    {
        MPINode master = entity.getConfig(MPINode.MPI_MASTER);
        SshMachineLocation master_machine = (SshMachineLocation) Iterables.filter(master.getLocations(),SshMachineLocation.class);


        log.info("Copying authorized_keys from master {} to slave {}" , master.getId(), entity.getId());
        master_machine.copyFrom("~/.ssh/authorized_keys","authorized_keys");

        log.info("Removing strict hosting in slave {}",entity.getId());
        getMachine().execCommands("remove strict hosting", ImmutableList.of(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config")));

        getMachine().copyTo(new File("authorized_keys"),"~/.ssh/authorized_keys");

    }
}
