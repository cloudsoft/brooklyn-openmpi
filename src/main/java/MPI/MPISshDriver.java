package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.java.JavaSoftwareProcessSshDriver;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.os.Os;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
import brooklyn.util.task.Tasks;
import brooklyn.util.text.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class MPISshDriver extends JavaSoftwareProcessSshDriver implements MPIDriver {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MPISshDriver.class);
    // TODO Need to store as attribute, so persisted and restored on brooklyn-restart
    private String connectivityTesterPath;


    public MPISshDriver(MPINodeImpl entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    protected String getLogFileLocation() {
        return String.format("%s/mpinode.log", getRunDir());
    }

    @Override
    public void install() {

        //attributeWhenReady(entity,MPINode.MPI_HOSTS);

        log.info("Waiting for any hosts to be ready...");


        log.info("MPI hosts ready");
        String debianFix = "export PATH=$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";


        log.info("waiting for hosts to be ready");

        //attributeWhenReady(entity,MPINode.MPI_HOSTS);
        String sgeConfigTemplate = processTemplate(entity.getConfig(MPINode.SGE_CONFIG_TEMPLATE_URL));


        getMachine().execCommands("create install dir", ImmutableList.of(format("mkdir -p %s", getInstallDir())));
        getMachine().copyTo(Streams.newInputStreamWithContents(sgeConfigTemplate), format("%s/sge.conf", getInstallDir()));


        log.info("installing SGE on {}", entity.getId());
        ScriptHelper installationScript = newScript(INSTALLING)
                .body.append(BashCommands.installJava7OrFail())
                .body.append(BashCommands.installPackage("build-essential"))
                .body.append(BashCommands.installPackage("libpam0g-dev"))
                .body.append(BashCommands.installPackage("libncurses5-dev"))
                .body.append(BashCommands.installPackage("csh"))
                .body.append(BashCommands.commandsToDownloadUrlsAs(ImmutableList.of("http://dl.dropbox.com/u/47200624/respin/ge2011.11.tar.gz"), format("%s/ge.tar.gz", getInstallDir())))

                        // TODO fix SGE functionality first then move on to MPI
                .body.append(format("mkdir -p %s", getGERunDir()))

                .body.append(format("tar xvfz %s/ge.tar.gz", getInstallDir()))
                .body.append(format("mv %s/ge2011.11/ %s", getInstallDir(), getSgeRoot()))

                .body.append(format("export SGE_ROOT=%s", getSgeRoot()))
                .body.append(format("useradd %s", getSgeAdmin()));


        log.info("MPI hosts are: {}", entity.getAttribute(MPINode.MPI_HOSTS));

        //  log.info("Done processing the template!");

        //  getMachine().execCommands("create GE Run Directory", ImmutableList.of(format("mkdir -p %s", getGERunDir())));
        //  getMachine().copyTo(Streams.newInputStreamWithContents(sgeConfigTemplate), format("%s/brooklynsgeconfig", getGERunDir()));


        installationScript
                .failOnNonZeroResultCode()
                .execute();


        connectivityTesterPath = Os.mergePathsUnix(getInstallDir(), "connectivity_c.c");


    }

    @Override
    public void customize() {

        //set java to use sun jre 6
        //customizeCmds.add(BashCommands.sudo("update-java-alternatives -s java-6-oracle"));


        //newScript(CUSTOMIZING)
        //        .body.append(customizeCmds)
        //        .failOnNonZeroResultCode()
        //        .execute();
    }


    @Override
    public Map<String, String> getShellEnvironment() {
        return super.getShellEnvironment();
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
//        int result = newScript(CHECK_RUNNING)
//                .body.append("mpicc " + connectivityTesterPath + " -o connectivity") // FIXME
//                .body.append("mpirun ./connectivity")
//                .execute();
        return (true);
    }

    @Override
    public void stop() {
        // TODO kill any jobs that are running
    }

    @Override
    public void updateHosts(List<String> mpiHosts) {

//        log.info("copying hosts inside ssh driver");
//        getMachine().copyTo(Streams.newInputStreamWithContents(Strings.join(mpiHosts, "\n")), "mpi_hosts");

        log.info("mpi hosts are : {}", entity.getAttribute(MPINode.MPI_HOSTS).get(0));
        //entity.setAttribute(MPINode.MPI_HOSTS,mpiHosts);
        log.info("mpi hosts are : {}", entity.getAttribute(MPINode.MPI_HOSTS));

    }

    private void uploadPublicKey(String publicKey) {

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
                    // commented because we do not need slave ssh keys to be copied to master
                    //       .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"")
                    //       .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                    .body.append("cat id_rsa.pub.txt >> ~/.ssh/authorized_keys")
                    .body.append("chmod 600 ~/.ssh/authorized_keys")
                    //       .body.append("cp ~/.ssh/id_rsa.pub id_rsa.pub." + entity.getId())
                    .failOnNonZeroResultCode()
                    .execute();


            //adding slave key to master
            //getMachine().copyFrom("id_rsa.pub." + entity.getId(), "id_rsa.pub." + entity.getId());

            //SshMachineLocation loc = (SshMachineLocation) Iterables.find(master.getLocations(), Predicates.instanceOf(SshMachineLocation.class));
            //loc.copyTo(new File("id_rsa.pub." + entity.getId()), "id_rsa.pub." + entity.getId());
            //loc.execCommands("add new slave ssh key to master",
            //        ImmutableList.of(
            //                "chmod 700 ~/.ssh/",
            //                "cat id_rsa.pub." + entity.getId() + " >> ~/.ssh/authorized_keys",
            //                "chmod 600 ~/.ssh/authorized_keys"));

        } catch (IOException i) {
            log.debug("Error parsing the file {}", i.getMessage());
        }

    }

    private String generateSshKey() {

        log.info("generating the master node ssh key on node: {}", entity.getId());


        //disables StrictHostKeyCheck and generates the master node key to allow communication between the nodes
        ScriptHelper foo = newScript("generateSshKey")
                .body.append("mkdir -p ~/.ssh/")
                .body.append("chmod 700 ~/.ssh")
                .body.append(BashCommands.executeCommandThenAsUserTeeOutputToFile("echo \"StrictHostKeyChecking no\"", "root", "/etc/ssh/ssh_config"))
                .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"")
                .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                .body.append("chmod 600 ~/.ssh/authorized_keys")
                .body.append("cp ~/.ssh/id_rsa.pub auth_keys.txt")
                .failOnNonZeroResultCode();

        foo.execute();
        foo.getResultStdout();

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

    public Integer getNumOfProcessors() {
        ScriptHelper fetchingProcessorsScript = newScript("gettingTheNoOfProcessors")
                .body.append("cat /proc/cpuinfo | grep processor | wc -l")
                .failOnNonZeroResultCode();

        fetchingProcessorsScript.execute();

        return Integer.parseInt(fetchingProcessorsScript.getResultStdout().split("\\n")[0].trim());
    }

    public String getMPIHosts() {
        return Strings.join(entity.getAttribute(MPINode.MPI_HOSTS), " ");
    }

    @Override
    public String getAdminHost() {
        return ((EntityInternal) entity.getConfig(MPINode.MPI_MASTER)).getAttribute(MPINode.HOSTNAME);
    }

    @Override
    public String getExecHosts() {

        return Strings.join(entity.getAttribute(MPINode.MPI_HOSTS), " ");
    }

    @Override
    public String getSubmissionHosts() {

        return Strings.join(entity.getAttribute(MPINode.MPI_HOSTS), " ");

    }


    private String getGERunDir() {
        return getRunDir() + "/GE";
    }

    private String getMPIRunDir() {
        return getRunDir() + "/open-mpi";
    }

    @Override
    public String getSgeRoot() {
        return "/opt/sge6";
    }

    @Override
    public String getSgeAdmin() {
        return "sgeadmin";
    }

    @Override
    public String getArch() {
        ScriptHelper getArchScript = newScript("querying SGE for arch").body.append(format("cd %s/ge*", getSgeRoot()))
                .body.append("./util/arch").failOnNonZeroResultCode();

        getArchScript.execute();

        return getArchScript.getResultStdout().split("\n")[0];
    }
}
