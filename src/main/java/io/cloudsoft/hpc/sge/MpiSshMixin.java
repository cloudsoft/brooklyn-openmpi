package io.cloudsoft.hpc.sge;

import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.ssh.BashCommands;
import com.google.common.collect.ImmutableList;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.lang.String.format;

public class MpiSshMixin {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MpiSshMixin.class);

    private final SgeSshDriver driver;
    private final SshMachineLocation machine;
    private final String openMpiVersion = "openmpi-1.6.5";

    public MpiSshMixin(SgeSshDriver driver, SshMachineLocation machine) {
        this.driver = driver;
        this.machine = machine;
    }

    //http://downloads.cloudsoftcorp.com/openmpi-1.6.5.tar.gz

    public List<String> installCommands() {

        //Entity entity = driver.getEntity();
        //ManagementContext managementContext = ((EntityInternal) driver.getEntity()).getManagementContext();

        //String mpiVersion = entity.getConfig(SgeNode.MPI_VERSION);
        //DownloadResolver mpiDownloadResolver = managementContext.getEntityDownloadsManager().newDownloader(
        //driver, "mpi", ImmutableMap.of("addonversion", mpiVersion));


        //List<String> mpiUrls = mpiDownloadResolver.getTargets();
        //String mpiSaveAs = mpiDownloadResolver.getFilename();


        return ImmutableList.<String>builder()
                //.addAll(mpiUrls)
                .addAll(BashCommands.commandsToDownloadUrlsAs(ImmutableList.of("http://downloads.cloudsoftcorp.com/openmpi-1.6.5.tar.gz"), format("%s/openmpi.tar.gz", driver.getInstallDir())))
                .add(format("mkdir -p /opt/"))
                .add(format("tar -C %s -xvf %s/openmpi.tar.gz", driver.getInstallDir(), driver.getInstallDir()))
                .add(format("mv %s/opt/%s /opt", driver.getInstallDir(), openMpiVersion))
                .build();

    }

    public List<String> customizeCommands() {
        //add the paths to bashrc.
        String openMpiPath = "/opt/" + openMpiVersion;

        return ImmutableList.of(format("echo \"export PATH=$PATH:%s\" >> /etc/bash.bashrc", openMpiPath + "/bin"),
                format("echo \"export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:%s\" >> /etc/bash.bashrc", openMpiPath + "/lib"));
    }
}
