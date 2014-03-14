package io.cloudsoft.hpc.sge;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
import brooklyn.util.task.DynamicTasks;
import brooklyn.util.task.Tasks;
import brooklyn.util.text.Strings;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.io.Charsets;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class SgeSshDriver extends VanillaSoftwareProcessSshDriver implements SgeDriver {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SgeSshDriver.class);
    //protected final MpiSshMixin mpiMixin;

    public SgeSshDriver(SgeNodeImpl entity, SshMachineLocation machine) {
        super(entity, machine);
        //mpiMixin = new MpiSshMixin(this, machine);
    }

    public static List<String> startNfsServer(String exportDir) {
        return ImmutableList.of(
                format("echo \"%s *(rw,sync,no_subtree_check,no_root_squash)\" >> /etc/exports", exportDir)
                , "/etc/init.d/portmap start"
                , "/etc/init.d/nfs-kernel-server start");
    }

//    @Override
//    protected String getLogFileLocation() {
//        return String.format("%s/sgenode.log", getRunDir());
//    }

    @Override
    public void install() {

        log.info("installing SGE on {}", entity.getId());

        List<String> genericInstallCommands = ImmutableList.of(BashCommands.installJava7OrFail(),
                BashCommands.installPackage("build-essential"),
                BashCommands.installPackage("libpam0g-dev"),
                BashCommands.installPackage("libncurses5-dev"),
                BashCommands.installPackage("csh"),
                format("export SGE_ROOT=%s", getSgeRoot()),
                format("useradd %s", getSgeAdmin())
        );

        if (isMaster()) {
            log.info("SGE hosts are: {}", entity.getAttribute(SgeNode.SGE_HOSTS));

            newScript(INSTALLING)
                    .failOnNonZeroResultCode()
                    .body.append(genericInstallCommands)
                    .body.append(BashCommands.INSTALL_TAR)
                    //install nfs server on master
                    .body.append(BashCommands.installPackage("nfs-kernel-server"))

                    // FIXME use same pattern as in Jboss7SshDriver.install, and Jboss7Serer.DOWNLOAD_URL
                    .body.append(BashCommands.commandsToDownloadUrlsAs(ImmutableList.of("http://dl.dropbox.com/u/47200624/respin/ge2011.11.tar.gz"), format("%s/ge.tar.gz", getInstallDir())))

                    // TODO fix io functionality first then move on to MPI
                    .body.append(format("tar xvfz %s/ge.tar.gz", getInstallDir()))
                    .body.append(format("mv %s/ge2011.11/ %s", getInstallDir(), getSgeRoot()))

                    //set the io admin to be the owner of the io root folder
                    .body.append(format("chown %s %s", getSgeAdmin(), getSgeRoot()))


                    //              .body.append(mpiMixin.installCommands())

                    .execute();

        } else {
            newScript(INSTALLING)
                    .failOnNonZeroResultCode()
                    .body.append(genericInstallCommands)
                    //install nfs common on slave
                    .body.append(BashCommands.installPackage("nfs-common"))
                    //            .body.append(mpiMixin.installCommands())
                    .execute();
        }

        //set the No of processors this machine has
        entity.setAttribute(SgeNode.NUM_OF_PROCESSORS, getNumOfProcessors());
    }

    public List<String> mountNfsDir(String host, String remoteDir, String mountPoint) {
        //FIXME modify fstab instead of directly mounting the volume
        return ImmutableList.of(
                "/etc/init.d/portmap start",
                format("mkdir -p %s", mountPoint),
                format("mount %s:%s %s", host, remoteDir, mountPoint)
        );
    }

    @Override
    public void customize() {
        // FIXME Remove duplication for master/slave paths?

        DynamicTasks.queueIfPossible(SshEffectorTasks.ssh(format("mkdir -p %s", getRunDir())).machine(getMachine()).summary("create the run dir"));

        //fetch the hostname alias for each (TODO: find alternatives for different clouds: non-ec2)
        ScriptHelper fetchAliasScript = newScript("fetching the alias for the node").body.append("cat /etc/hostname").gatherOutput(true);
        fetchAliasScript.execute();

        entity.setAttribute(SgeNode.SGE_NODE_ALIAS, Optional.of(fetchAliasScript.getResultStdout().split("\\n")[0]).get());

        if (isMaster()) {

            //get master's hostname
            String alias = entity.getAttribute(SgeNode.SGE_NODE_ALIAS);

            //process the sge config template to have the master nodes configurations
            String sgeConfigTemplate = processTemplate(entity.getConfig(SgeNode.SGE_CONFIG_TEMPLATE_URL),
                    ImmutableMap.of("exec_hosts", alias, "admin_hosts", alias, "submit_hosts", alias));

            DynamicTasks.queueIfPossible(SshEffectorTasks.put(format("%s/sge_setup.conf", getRunDir()))
                    .contents(Streams.newInputStreamWithContents(sgeConfigTemplate))
                    .machine(getMachine())
                    .summary("sending the processed sge setup config template file to the machine"));

            //process the sge profile template
            String sgeProfileTemplate = processTemplate(entity.getConfig(SgeNode.SGE_PROFILE_TEMPLATE_URL));

            DynamicTasks.queueIfPossible(SshEffectorTasks.put(format("%s/sge_profile.conf", getRunDir()))
                    .contents(Streams.newInputStreamWithContents(sgeProfileTemplate))
                    .machine(getMachine())
                    .summary("sending the processed profile template to the machine"));


            DynamicTasks.queueIfPossible(newScript(CUSTOMIZING)
                    .failOnNonZeroResultCode()
                    .body.append(startNfsServer(getSgeRoot()))
                    .body.append(format("export SGE_ROOT=%s", getSgeRoot()))
                            //copy the sge_setup.conf to SGE_ROOT
                    .body.append(format("cp %s/sge_setup.conf %s/", getRunDir(), getSgeRoot()))
                    .body.append(format("cd %s && TERM=rxvt ./inst_sge -m -x -noremote -jmx -auto %s/sge_setup.conf", getSgeRoot(), getRunDir()))
                            //          .body.append(mpiMixin.customizeCommands())
                    .body.append(format("cat %s/sge_profile.conf >> /etc/bash.bashrc", getRunDir()))
                    .body.append(format("source /etc/bash.bashrc"))
                    .newTask());


        } else {

            String alias = entity.getAttribute(SgeNode.SGE_NODE_ALIAS);
            // wait for the master, so our mount point and inst_sge etc are ready
            // TODO assumes that SgeMaster is configured identically, to have the same dir exported as we want to mount
            SgeNode master = entity.getAttribute(SgeNode.SGE_MASTER);
            attributeWhenReady(master, SgeNode.SERVICE_UP);

            //mount master's sge root on NFS
            String masterHostname = master.getAttribute(SgeNode.HOSTNAME);
            String masterDir = getSgeRoot();
            DynamicTasks.queueIfPossible(SshEffectorTasks
                    .ssh(mountNfsDir(masterHostname, masterDir, getSgeRoot()))
                    .machine(getMachine())
                    .summary(format("mounting SGE_ROOT on slave: %s", entity.getId()))
                    .newTask());

            DynamicTasks.queueIfPossible(Entities.invokeEffectorWithArgs(entity, master, SgeNode.ADD_SLAVE, entity));


            //process the sge config template to have the master nodes configurations
            String sgeConfigTemplate = processTemplate(entity.getConfig(SgeNode.SGE_CONFIG_TEMPLATE_URL),
                    ImmutableMap.of("exec_hosts", alias, "admin_hosts", "", "submit_hosts", ""));


            //process the profile template for this entity
            String sgeProfileTemplate = processTemplate(entity.getConfig(SgeNode.SGE_PROFILE_TEMPLATE_URL));

            DynamicTasks.queueIfPossible(SshEffectorTasks.put(format("%s/sge_setup.conf", getRunDir())).contents(sgeConfigTemplate).machine(getMachine()).summary("copying the sge setup template to the machine"));
            DynamicTasks.queueIfPossible(SshEffectorTasks.put(format("%s/sge_profile.conf", getRunDir())).contents(sgeProfileTemplate).machine(getMachine()).summary("copying the sge profile file to the machine"));

            DynamicTasks.queueIfPossible(newScript(CUSTOMIZING)
                    .body.append(format("cd %s && TERM=rxvt ./inst_sge -x -noremote -auto %s/sge_setup.conf", getSgeRoot(), getRunDir()))
                    .body.append(format("cat %s/sge_profile.conf >> /etc/bash.bashrc", getRunDir()))
                    .body.append(format("source /etc/bash.bashrc"))
                    .newTask());

        }
    }


    @Override
    public Map<String, String> getShellEnvironment() {
        return super.getShellEnvironment();
    }

    protected boolean isMaster() {
        return Boolean.TRUE.equals(entity.getAttribute(SgeNode.MASTER_FLAG));
    }


    @Override
    public void launch() {


        //if entity is master generates the master ssh key and sets the master flag to be set.
        if (isMaster()) {
            log.info("Entity: {} is master now generateing Ssh key", entity.getId());
            String publicKey = generateSshKey();
            entity.setAttribute(SgeNode.MASTER_PUBLIC_SSH_KEY, publicKey);


            //setup the initial Parallel environment
            setupPE();

        } else {
            //wait for master ssh key to be set before executing.

            SgeNode master = entity.getAttribute(SgeNode.SGE_MASTER);
            log.info("Entity: {} is not master copying Ssh key from master (when available) {}", entity.getId(), master);

            String publicKey = attributeWhenReady(master, SgeNode.MASTER_PUBLIC_SSH_KEY);
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

        if (isMaster()) {
            newScript(ImmutableMap.of("usePidFile", false), STOPPING)
                    .body.append("pkill -9 sge_execd")
                    .body.append("pkill -9 sge_qmaster")
                    .execute();
        } else {

            newScript(ImmutableMap.of("usePidFile", false), STOPPING)
                    .body.append("pkill -9 sge_execd")
                    .execute();
        }
    }

    @Override
    public void addSlave(SgeNode slave) {

        String hostname = slave.getAttribute(SgeNode.HOSTNAME);
        String alias = slave.getAttribute(SgeNode.SGE_NODE_ALIAS);

        log.warn("adding new slave {} to the SGE Cluster", slave.getId());

        //add slave to master as submission and admin host;
        DynamicTasks.queueIfPossible(newScript(format("adding slave %s (%s) to queue", hostname, alias))
                .body.append(format("%s -as %s", qconf(), alias))
                .body.append(format("%s -ah %s", qconf(), alias))
                .gatherOutput(true)
                .newTask());

        //TODO update PE with the new Slave data
    }

    @Override
    public void removeSlave(SgeNode slave) {
        //TODO update this method and update PE based on the removal
        String hostname = slave.getAttribute(SgeNode.HOSTNAME);

        newScript(format("removing slave %s from SGE queue", hostname))
                .body.append(format("%s -dattr hostgroup hostlist %s @allhosts", qconf(), hostname))
                .body.append(format("%s -purge queue slots all.q@%s", qconf(), hostname))
                .body.append(format("%s -dconf %s", qconf(), hostname))
                .body.append(format("%s -de %s", qconf(), hostname))
                .execute();


    }


    private void uploadPublicKey(String publicKey) {

        try {

            byte[] encoded = Files.toByteArray(new File(publicKey));
            String myKey = Charsets.UTF_8.decode(ByteBuffer.wrap(encoded)).toString();

            getMachine().copyTo(Streams.newInputStreamWithContents(myKey), "id_rsa.pub.txt");
            //get master node
            SgeNode master = entity.getAttribute(SgeNode.SGE_MASTER);

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

        if (entity.getAttribute(SgeNode.NUM_OF_PROCESSORS) == null) {
            ScriptHelper fetchingProcessorsScript = newScript("gettingTheNoOfProcessors")
                    .body.append("cat /proc/cpuinfo | grep processor | wc -l")
                    .failOnNonZeroResultCode();

            fetchingProcessorsScript.execute();

            return Integer.parseInt(fetchingProcessorsScript.getResultStdout().split("\\n")[0].trim());
        } else {
            return entity.getAttribute(SgeNode.NUM_OF_PROCESSORS);
        }

    }


    // FIXME Use config key
    @Override
    public String getSgeRoot() {
        String sgeRoot = entity.getConfig(SgeNode.SGE_ROOT);
        int lastIndex = sgeRoot.length() - 1;
        if (sgeRoot.charAt(lastIndex) == '/') {

            return sgeRoot.substring(0, lastIndex);

        } else {
            return sgeRoot;
        }
    }

    @Override
    public String getSgeAdmin() {
        return entity.getConfig(SgeNode.SGE_ADMIN);
    }

    @Override
    public String getArch() {

        if (entity.getAttribute(SgeNode.SGE_ARCH) == null) {
            ScriptHelper getArchScript = newScript("querying io for arch").body.append(format("%s/util/arch", getSgeRoot()))
                    .failOnNonZeroResultCode();

            getArchScript.execute();

            return getArchScript.getResultStdout().split("\n")[0];
        } else {
            return entity.getAttribute(SgeNode.SGE_ARCH);
        }
    }

    @Override
    public String getPeName() {
        return (Preconditions.checkNotNull(entity.getConfig(SgeNode.SGE_PE_NAME), "PE attribute is not set yet."));
    }


    public SgeNode getMaster() {
        if (Boolean.TRUE.equals(entity.getConfig(SgeNode.MASTER_FLAG)))
            return (SgeNode) entity;
        else
            return entity.getAttribute(SgeNode.SGE_MASTER);
    }

//    public boolean peExists(String peName) {
//        int exitStatus = newScript("check if the parallel environment exist")
//                .body.append(setEnv())
//                .body.append(format("%s -sp %s", qconf(), peName))
//                .execute();
//        if (exitStatus != 0)
//            log.warn("qconf returned non-zero eit status: " + exitStatus);
//
//        return (exitStatus == 0);
//    }

    @Override
    public void updatePE(String peName, Integer numOfProcessors) {
        log.info("updating pe: {} with num of processors: {}", peName, numOfProcessors);

        newScript("updating the number of processors in the PE")
                .body.append(setEnv())
                .body.append(format("%s -mattr pe slots %s %s", qconf(), numOfProcessors, peName))
                .failOnNonZeroResultCode()
                .execute();


    }


    public void setupPE() {

        //process the parallel environment template
        String sgePETemplate = processTemplate(entity.getConfig(SgeNode.SGE_PE_TEMPLATE_URL));

        DynamicTasks.queueIfPossible(SshEffectorTasks.put(format("%s/sge_pe.conf", getRunDir()))
                .contents(sgePETemplate)
                .summary("copying the PE configuration file")
                .machine(getMachine()));

        DynamicTasks.queueIfPossible(newScript("creating new parallel environment")
                .body.append(setEnv())
                .body.append(format("%s -Ap %s/sge_pe.conf", qconf(), getRunDir())).newTask());


    }

    //returns the qconf command with PATH
    public String qconf() {
//        String sgeRoot = getSgeRoot();
//        String arch = format("`%s/util/arch`", sgeRoot);

        String qconfCmd = Strings.join(ImmutableList.of(format("source %s/sge_profile.conf;", getRunDir()), " qconf"), "");

        return (qconfCmd);

    }

    //returns the commands necessary to get the environment variables
    public String setEnv() {
        return (format("source %s/sge_profile.conf", getRunDir()));
    }


}
