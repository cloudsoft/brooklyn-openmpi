package MPI;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
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
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

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

        //set num of processors
        //log.info("setting the number of processors for {} ",entity.getId());

        //entity.setAttribute(MPINode.NUM_OF_PROCESSORS,getNumOfProcessors());

        String debianFix = "export PATH=$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

        log.info("copying open-mpi and SGE binaries across to {}" , entity.getId());

        getMachine().copyTo(new File(getClass().getClassLoader().getResource("ge2011.11p-linux64.tar.gz").getFile()),getInstallDir() + "/ge.tar.gz");
        getMachine().copyTo(new File(getClass().getClassLoader().getResource("openmpi-1.6.5-withsge.tar.gz").getFile()), getInstallDir() + "/openmpi.tar.gz");

        log.info("installing SGE on {}",entity.getId());
        ScriptHelper installationScript = newScript(INSTALLING)
                .body.append(BashCommands.installJava7OrFail())
                .body.append(BashCommands.installPackage("build-essential"))
                .body.append(BashCommands.installPackage("libpam0g-dev"))
                .body.append(BashCommands.installPackage("libncurses5-dev"))
                .body.append(BashCommands.installPackage("csh"))
                //.body.append(BashCommands.commandToDownloadUrlAs("http://www.open-mpi.org/software/ompi/v1.6/downloads/openmpi-1.6.5.tar.gz","openmpi.tar.gz"))
                //.body.append(BashCommands.commandToDownloadUrlAs("http://sourceforge.net/projects/gridscheduler/files/GE2011.11p1/GE2011.11p1.tar.gz/download?use_mirror=autoselect","ge.tar.gz"))
                //.body.append(BashCommands.commandToDownloadUrlAs("http://svn.open-mpi.org/svn/ompi/tags/v1.6-series/v1.6.4/examples/connectivity_c.c", connectivityTesterPath))
                //.body.append(format("cd %s", getInstallDir()))
                //.body.append(format("tar xvfz ge.tar.gz"))
                .body.append(format("mkdir -p %s/GEBinaries"),getInstallDir())

                .body.append(format("mkdir -p %s"), getGERunDir())
                .body.append(format("mkdir -p %s"), getMPIRunDir())

                .body.append(format("tar xvfz %s/ge.tar.gz -C %s/GEBinaries",getInstallDir(),getInstallDir()))
                .body.append(format("tar xvfz %s/openmpi.ta.gz -C %s",getInstallDir(),getMPIRunDir()))

                .body.append(format("export SGE_ROOT=%s"),getGERunDir())
                .body.append(format("%s/GEBinaries/scripts/distinst -all -local -noexit -y",getInstallDir()))

                //.body.append(format("cd GE*/source"))
                //.body.append(format("./aimk -no-java -no-jni -no-secure -spool-classic -no-dump -only-depend"))
                //.body.append(format("./scripts/zerodepend"))
                //.body.append(format("./aimk -no-java -no-jni -no-secure -spool-classic -no-dump depend"))
                //.body.append(format("./aimk -no-java -no-jni -no-secure -spool-classic -no-dump -no-qmon"))
                .failOnNonZeroResultCode();
        //wait for MPI_HOSTS to be available
        //attributeWhenReady(entity,MPINode.MPI_HOSTS);

        installationScript.execute();


        log.info("MPI hosts are: {}" , entity.getAttribute(MPINode.MPI_HOSTS));

//        String sgeConfigTemplate = processTemplate(entity.getConfig(MPINode.SGE_CONFIG_TEMPLATE_URL));
//
//        getMachine().copyTo(Streams.newInputStreamWithContents(sgeConfigTemplate), format("%s/brooklynsgeconfig", getRunDir()));
//
//        String SGEinstallationCmd = "";
//        if(((MPINode) entity).isMaster())
//            SGEinstallationCmd = "./inst_sge -m -noremote -auto ./brooklynsgeconfig";
//        else
//            SGEinstallationCmd = "./inst_sge -auto ./brooklynsgeconfig";
//
//            installationScript.body.append(format("cd %s"),getRunDir());
//            installationScript.body.append(SGEinstallationCmd)
//                    .failOnNonZeroResultCode()
//                    .execute();



                //automatically accept the liscence agreement
//                "sudo echo oracle-java6-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections",

//                BashCommands.sudo("add-apt-repository ppa:webupd8team/java -y"),
//                BashCommands.sudo("apt-get update"),
//                BashCommands.installPackage("oracle-java6-installer"),
//                BashCommands.installPackage("oracle-java6-set-default"),

                //install open-mpi
//                BashCommands.installPackage("openmpi-bin"),
//                BashCommands.installPackage("openmpi-checkpoint"),
//                BashCommands.installPackage("openmpi-common"),
//                BashCommands.installPackage("openmpi-doc"),
//                BashCommands.installPackage("libopenmpi-dev"),

                //install pam dev package

                //install curses package for qtcsh
                //install csh
                //get the files



                //"tar xvfz openmpi.tar.gz -C ~/openmpi",
                //"tar xvfz ge.tar.gz -C ~/ge");



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
    public void updateHostsFile(List<String> mpiHosts) {

            log.info("copying hosts inside ssh driver");
            getMachine().copyTo(Streams.newInputStreamWithContents(Strings.join(mpiHosts, "\n")), "mpi_hosts");

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

        log.info("generating the master node ssh key on node: {}",entity.getId());


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

    public Integer getNumOfProcessors()
    {
        ScriptHelper fetchingProcessorsScript = newScript("gettingTheNoOfProcessors")
                .body.append("cat /proc/cpuinfo | grep processor | wc -l")
                .failOnNonZeroResultCode();

        fetchingProcessorsScript.execute();

        return Integer.parseInt(fetchingProcessorsScript.getResultStdout().split("\\n")[0].trim());
    }

    public String getMPIHosts()
    {
        return entity.getAttribute(MPINode.MPI_HOSTS);
    }

    public String getAdminHost()
    {
        return ((EntityInternal) entity.getConfig(MPINode.MPI_MASTER)).getAttribute(MPINode.HOSTNAME);
    }

    public String getExecHosts()
    {
        return "\"" + ((EntityInternal) entity.getConfig(MPINode.MPI_MASTER)).getAttribute(MPINode.HOSTNAME) + "\"" ;
    }

    public String getSubmissionHost()
    {
        return entity.getAttribute(MPINode.MPI_HOSTS);
    }


    private String getGERunDir()
    {
        return getRunDir() + "/GE";
    }

    private String getMPIRunDir()
    {
        return getRunDir() + "/open-mpi";
    }

}
