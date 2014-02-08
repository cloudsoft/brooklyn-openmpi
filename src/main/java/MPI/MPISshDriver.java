package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.os.Os;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
import brooklyn.util.task.Tasks;
import brooklyn.util.text.Identifiers;
import brooklyn.util.text.Strings;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPISshDriver extends VanillaSoftwareProcessSshDriver implements MPIDriver {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPISshDriver.class);
    // TODO Need to store as attribute, so persisted and restored on brooklyn-restart
    private String connectivityTesterPath;

    public MPISshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public void install() {
        super.install();

        connectivityTesterPath = Os.mergePathsUnix(getInstallDir(), "connectivity_c.c");
        newScript(INSTALLING)
                .body.append("sudo ufw disable")
                .body.append(BashCommands.installPackage("build-essential"))
                .body.append(BashCommands.installPackage("openmpi-bin"))
                .body.append(BashCommands.installPackage("openmpi-checkpoint"))
                .body.append(BashCommands.installPackage("openmpi-common"))
                .body.append(BashCommands.installPackage("openmpi-doc"))
                .body.append(BashCommands.installPackage("libopenmpi-dev"))
                .body.append(BashCommands.commandToDownloadUrlAs("http://svn.open-mpi.org/svn/ompi/tags/v1.6-series/v1.6.4/examples/connectivity_c.c", connectivityTesterPath))
                .failOnNonZeroResultCode()
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


        //if entity is master..
        if (Boolean.TRUE.equals(entity.getAttribute(MPINode.MASTER_FLAG))) {
            log.info("Entity: {} is master now generateing Ssh key", entity.getId());
            String publicKey = generateSshKey();
            entity.setAttribute(MPINode.MASTER_PUBLIC_SSH_KEY, publicKey);

        } else {
            //wait for master ssh key to be set before executing.

            MPINode master = entity.getConfig(MPINode.MPI_MASTER);
            log.info("Entity: {} is not master copying Ssh key from master (when available) {}", entity.getId(), master);

            String publicKey = attributeWhenReady(master, MPINode.MASTER_PUBLIC_SSH_KEY);
            uploadPublicKey(publicKey);
        }
    }

    @Override
    public boolean isRunning() {
        int result = newScript(CHECK_RUNNING)
                .body.append("mpicc " + connectivityTesterPath + " -o connectivity") // FIXME
                .body.append("mpirun ./connectivity")
                .execute();
        return (result == 0);
    }

    @Override
    public void stop() {
        // TODO kill any jobs that are running
    }

    @Override
    public void updateHostsFile(List<String> mpiHosts) {
        log.info("updateHostsFile invoked");

        getMachine().copyTo(Streams.newInputStreamWithContents(Strings.join(mpiHosts, "\n")), "mpi_hosts");
    }

    private void uploadPublicKey(String publicKey) {
        //log.info("Copying authorized_keys from master {} to slave {}", master.getId(), entity.getId());

        //copy rsa files across from master to local
        //master_machine.

        //send files from local to slave
        try {


            byte[] encoded = Files.toByteArray(new File(publicKey));
            String myKey = Charsets.UTF_8.decode(ByteBuffer.wrap(encoded)).toString();

            getMachine().copyTo(Streams.newInputStreamWithContents(myKey), "id_rsa.pub.txt");

            //get master node
            MPINode master = entity.getConfig(MPINode.MPI_MASTER);

            newScript("uploadMasterSshKey")
                    .body.append("mkdir -p ~/.ssh/")
                    .body.append("chmod 700 ~/.ssh/")
                    .body.append(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"))
                    .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"")
                    .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                    .body.append("cat id_rsa.pub.txt >> ~/.ssh/authorized_keys")
                    //.body.append("cat ~/.ssh/id_rsa.pub | ssh $USER@" + masterAddress + " \"mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys\"")
                    .body.append("chmod 600 ~/.ssh/authorized_keys")
                    .body.append("cp ~/.ssh/id_rsa.pub id_rsa.pub." + entity.getId())
                    .failOnNonZeroResultCode()
                    .execute();


            //adding slave key to master
            getMachine().copyFrom("id_rsa.pub." + entity.getId(), "id_rsa.pub." + entity.getId());

            SshMachineLocation loc = (SshMachineLocation) Iterables.find(master.getLocations(), Predicates.instanceOf(SshMachineLocation.class));
            loc.copyTo(new File("id_rsa.pub." + entity.getId()), "id_rsa.pub." + entity.getId());
            loc.execCommands("add new slave ssh key to master",
                    ImmutableList.of(
                            "chmod 700 ~/.ssh/",
                            "cat id_rsa.pub." + entity.getId() + " >> ~/.ssh/authorized_keys",
                            "chmod 600 ~/.ssh/authorized_keys"));

        } catch (IOException i) {
            log.debug("Error parsing the file {}", i.getMessage());
        }

    }

    private String generateSshKey() {

        log.info("generating the master node ssh key");


        newScript("generateSshKey")
                .body.append("mkdir -p ~/.ssh/")
                .body.append("chmod 700 ~/.ssh")
                .body.append(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"))
                .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"")
                .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                .body.append("chmod 600 ~/.ssh/authorized_keys")
                .body.append("cp ~/.ssh/id_rsa.pub auth_keys.txt")
                .failOnNonZeroResultCode()
                .execute();

        getMachine().copyFrom("auth_keys.txt", "auth_keys.txt");

        return "auth_keys.txt";
    }

    @Override
    public void simpleCompile(String inputUrl) {
        String cfile = inputUrl.substring(inputUrl.lastIndexOf("/") + 1);
        if (Strings.isBlank(cfile)) cfile = Identifiers.makeRandomId(8) + ".c";
        String ofile = (cfile.contains(".") ? cfile.substring(0, cfile.lastIndexOf(".")) : cfile) + ".o";

        newScript("compileAndRun")
                .body.append(BashCommands.commandsToDownloadUrlsAs(Lists.newArrayList(inputUrl), cfile))
                .body.append("mpicc " + cfile + " -o " + ofile)
                .body.append("mpirun " + ofile)
                .failOnNonZeroResultCode()
                .execute();
    }

    @SuppressWarnings("unchecked")
    private <T> T attributeWhenReady(Entity target, AttributeSensor<T> sensor) {
        try {
            return (T) Tasks.resolveValue(
                    DependentConfiguration.attributeWhenReady(target, sensor),
                    sensor.getType(),
                    ((EntityInternal) entity).getExecutionContext(),
                    "Getting " + sensor + " from " + target);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
}
