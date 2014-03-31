package io.cloudsoft.hpc.mpi;

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
import brooklyn.util.text.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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
        // FIXME debian wheezy didn't run apt-get without exporting the PATH first.
        String debianFix = "export PATH=$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
        List<String> installCmds = Lists.newArrayList(
                debianFix,
                BashCommands.installPackage("build-essential"),
                BashCommands.installPackage("openmpi-bin"),
                BashCommands.installPackage("openmpi-checkpoint"),
                BashCommands.installPackage("openmpi-common"),
                BashCommands.installPackage("openmpi-doc"),
                BashCommands.installPackage("libopenmpi-dev"),
                BashCommands.commandToDownloadUrlAs("http://svn.open-io.cloudsoft.hpc.mpi.org/svn/ompi/tags/v1.6-series/v1.6.4/examples/connectivity_c.c", connectivityTesterPath));


        connectivityTesterPath = Os.mergePathsUnix(getInstallDir(), "connectivity_c.c");

        newScript(INSTALLING)

                .body.append(installCmds)
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



        //if entity is master generates the master ssh key and sets the master flag to be set.
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
        return (true);
    }

    @Override
    public void stop() {
        // TODO kill any jobs that are running
    }

    @Override
    public void updateHostsFile(List<String> mpiHosts) {

            getMachine().copyTo(Streams.newInputStreamWithContents(Strings.join(mpiHosts, "\n")), "mpi_hosts");
    }

    private void uploadPublicKey(String publicKey) {

        try {

            byte[] encoded = Files.toByteArray(new File(publicKey));
            String myKey = Charsets.UTF_8.decode(ByteBuffer.wrap(encoded)).toString();

            getMachine().copyTo(Streams.newInputStreamWithContents(myKey), "id_rsa.pub.txt");

            newScript("uploadMasterSshKey")
                    .body.append("mkdir -p ~/.ssh/")
                    .body.append("chmod 700 ~/.ssh/")
                    .body.append(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"))
                    .body.append("cat id_rsa.pub.txt >> ~/.ssh/authorized_keys")
                    .body.append("chmod 600 ~/.ssh/authorized_keys")
                    .failOnNonZeroResultCode()
                    .execute();


        } catch (IOException i) {
            log.debug("Error parsing the file {}", i.getMessage());
        }

    }

    private String generateSshKey() {

        log.info("generating the master node ssh key on node: {}",entity.getId());


        //disables StrictHostKeyCheck and generates the master node key to allow communication between the nodes
        newScript("generateSshKey")
                .body.append("mkdir -p ~/.ssh/")
                .body.append("chmod 700 ~/.ssh")
                .body.append(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"))
                .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open io.cloudsoft.hpc.mpi\" -P \"\"")
                .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                .body.append("chmod 600 ~/.ssh/authorized_keys")
                .body.append("cp ~/.ssh/id_rsa.pub auth_keys.txt")
                .failOnNonZeroResultCode()
                .execute();

        getMachine().copyFrom("auth_keys.txt", "auth_keys.txt");

        return "auth_keys.txt";
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
