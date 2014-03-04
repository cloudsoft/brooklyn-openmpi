package io.cloudsoft.hpc.sge;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.java.JavaSoftwareProcessSshDriver;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.DependentConfiguration;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.stream.Streams;
import brooklyn.util.task.Tasks;
import brooklyn.util.text.Strings;
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

public class SgeSshDriver extends JavaSoftwareProcessSshDriver implements SgeDriver {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SgeSshDriver.class);
    // TODO Need to store as attribute, so persisted and restored on brooklyn-restart
    private String connectivityTesterPath;

    protected final MpiSshMixin mpiMixin;

    public SgeSshDriver(SgeNodeImpl entity, SshMachineLocation machine) {
        super(entity, machine);
        mpiMixin = new MpiSshMixin(this, machine);
    }

    @Override
    protected String getLogFileLocation() {
        return String.format("%s/mpinode.log", getRunDir());
    }

    @Override
    public void install() {
        log.info("waiting for hosts to be ready");

        String sgeConfigTemplate = processTemplate(entity.getConfig(SgeNode.SGE_CONFIG_TEMPLATE_URL));


        getMachine().execCommands("create install dir", ImmutableList.of(format("mkdir -p %s", getInstallDir())));
        getMachine().copyTo(Streams.newInputStreamWithContents(sgeConfigTemplate), format("%s/io.conf", getInstallDir()));


        log.info("installing io on {}", entity.getId());


        List<String> genericInstallCommands = ImmutableList.of(BashCommands.installJava7OrFail(),
                BashCommands.installPackage("build-essential"),
                BashCommands.installPackage("libpam0g-dev"),
                BashCommands.installPackage("libncurses5-dev"),
                BashCommands.installPackage("csh"));

        if (isMaster()) {
            log.info("MPI hosts are: {}", entity.getAttribute(SgeNode.SGE_HOSTS));

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

                    .body.append(format("export SGE_ROOT=%s", getSgeRoot()))
                    .body.append(format("useradd %s", getSgeAdmin()))

                    //set the io admin to be the owner of the io root folder
                    .body.append(format("chown %s %s",getSgeAdmin(),getSgeRoot()))


                    .body.append(mpiMixin.installCommands())

                    .execute();

        } else {
            newScript(INSTALLING)
                    .failOnNonZeroResultCode()
                    .body.append(genericInstallCommands)
                    //install nfs common on slave
                    .body.append(BashCommands.installPackage("nfs-common"))
                    .body.append(mpiMixin.installCommands())
                    .execute();
        }
    }

    public static List<String> startNfsServer(String exportDir) {
        throw new UnsupportedOperationException();
    }

    public static List<String> mountNfsDir(String host, String remoteDir, String mountPoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void customize() {
        log.info("Waiting for any hosts to be ready, in {}", this);
        attributeWhenReady(entity, SgeNode.SGE_HOSTS);
        log.info("MPI hosts ready, in {}", this);

        String sgeConfigTemplate = processTemplate(entity.getConfig(SgeNode.SGE_CONFIG_TEMPLATE_URL));

        getMachine().execCommands("create run dir", ImmutableList.of(format("mkdir -p %s", getRunDir())));
        getMachine().copyTo(Streams.newInputStreamWithContents(sgeConfigTemplate), format("%s/io.conf", getRunDir()));

        // FIXME Remove duplication for master/slave paths?
        if (isMaster()) {
            newScript(CUSTOMIZING)
                    .failOnNonZeroResultCode()
                    .body.append(startNfsServer(getSgeRoot()))
                    .body.append(format("export SGE_ROOT=%s", getSgeRoot()))
                    .body.append(format("cd %s && TERM=rxvt ./inst_sge -m -x -noremote -auto %s/io.conf",getSgeRoot(), getRunDir()))
                    .body.append(mpiMixin.customizeCommands())
                    .execute();
        } else {
            // wait for the master, so our mount point and inst_sge etc are ready
            // TODO assumes that SgeMaster is configured identically, to have the same dir exported as we want to mount
            SgeNode master = entity.getConfig(SgeNode.SGE_MASTER);
            attributeWhenReady(master, SgeNode.SERVICE_UP);

            String masterHostname = master.getAttribute(SgeNode.HOSTNAME);
            String masterDir = getSgeRoot();

            newScript(CUSTOMIZING)
                    .failOnNonZeroResultCode()
                    .body.append(mountNfsDir(masterHostname, masterDir, getSgeRoot()))
                    .body.append(format("export SGE_ROOT=%s", getSgeRoot()))
                    .body.append(format("cd %s && TERM=rxvt ./inst_sge -x -noremote -auto %s/io.conf", getSgeRoot(), getRunDir()))
                    .body.append(mpiMixin.customizeCommands())
                    .execute();
        }


    /*
        export SGE_ROOT="/opt/sge6"
        export SGE_CELL="default"
        export SGE_CLUSTER_NAME="brooklyncluster"
        export SGE_QMASTER_PORT="63231"
        export SGE_EXECD_PORT="63232"
        export MANTYPE="man"
        export MANPATH="$MANPATH:$SGE_ROOT/man"
        export PATH="$PATH:$SGE_ROOT/bin/linux-x64"
        export ROOTPATH="$ROOTPATH:$SGE_ROOT/bin/linux-x64"
        export LDPATH="$LDPATH:$SGE_ROOT/lib/linux-x64"
        export DRMAA_LIBRARY_PATH="$SGE_ROOT/lib/linux-x64/libdrmaa.so"
    */
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

        } else {
            //wait for master ssh key to be set before executing.

            SgeNode master = entity.getConfig(SgeNode.SGE_MASTER);
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
        newScript(ImmutableMap.of("usePidFile", false), STOPPING)
                .body.append("pkill -9 sge_execd")
                .execute();
    }

    @Override
    public void addSlave(String alias) {
        // FIXME Do:
//        mssh.execute('qconf -as %s' % node.alias)
//
//        def _add_sge_admin_host(self, node):
//        mssh = self._master.ssh
//        mssh.execute('qconf -ah %s' % node.alias)
    }

    @Override
    public void removeSlave(String alias) {
        // FIXME put in:
//        master.ssh.execute('qconf -dattr hostgroup hostlist %s @allhosts' %
//                node.alias)
//        master.ssh.execute('qconf -purge queue slots all.q@%s' % node.alias)
//        master.ssh.execute('qconf -dconf %s' % node.alias)
//        master.ssh.execute('qconf -de %s' % node.alias)
    }

    @Override
    public void updateHosts(List<String> mpiHosts) {

//        log.info("copying hosts inside ssh driver");
//        getMachine().copyTo(Streams.newInputStreamWithContents(Strings.join(mpiHosts, "\n")), "mpi_hosts");

        log.info("mpi hosts are : {}", entity.getAttribute(SgeNode.SGE_HOSTS).get(0));
        //entity.setAttribute(SgeNode.MPI_HOSTS,mpiHosts);
        log.info("mpi hosts are : {}", entity.getAttribute(SgeNode.SGE_HOSTS));

    }

    private void uploadPublicKey(String publicKey) {

        try {

            byte[] encoded = Files.toByteArray(new File(publicKey));
            String myKey = Charsets.UTF_8.decode(ByteBuffer.wrap(encoded)).toString();

            getMachine().copyTo(Streams.newInputStreamWithContents(myKey), "id_rsa.pub.txt");
            //get master node
            SgeNode master = entity.getConfig(SgeNode.SGE_MASTER);

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
        return Strings.join(entity.getAttribute(SgeNode.SGE_HOSTS), " ");
    }

    @Override
    public String getAdminHost() {
        return getMaster().getAttribute(SgeNode.HOSTNAME);
    }

    @Override
    public String getExecHosts() {
        return Strings.join(entity.getAttribute(SgeNode.SGE_HOSTS), " ");
    }

    @Override
    public String getSubmissionHosts() {
        return Strings.join(entity.getAttribute(SgeNode.SGE_HOSTS), " ");

    }


    private String getGERunDir() {
        return getRunDir() + "/GE";
    }

    private String getMPIRunDir() {
        return getRunDir() + "/open-mpi";
    }

    // FIXME Use config key
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
        ScriptHelper getArchScript = newScript("querying io for arch").body.append(format("cd %s/ge*", getSgeRoot()))
                .body.append("./util/arch").failOnNonZeroResultCode();

        getArchScript.execute();

        return getArchScript.getResultStdout().split("\n")[0];
    }

    public SgeNode getMaster()
    {
        if (Boolean.TRUE.equals(entity.getConfig(SgeNode.MASTER_FLAG)))
            return (SgeNode) entity;
        else
            return entity.getConfig(SgeNode.SGE_MASTER);
    }
}
