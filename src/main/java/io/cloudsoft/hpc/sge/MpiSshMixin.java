package io.cloudsoft.hpc.sge;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.entity.drivers.EntityDriver;
import brooklyn.entity.drivers.downloads.DownloadResolver;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.management.ManagementContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.lang.String.format;

public class MpiSshMixin {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MpiSshMixin.class);

    private final SgeSshDriver driver;
    private final SshMachineLocation machine;

    public MpiSshMixin(SgeSshDriver driver, SshMachineLocation machine) {
        this.driver = driver;
        this.machine = machine;
    }

    public List<String> installCommands() {
        if (driver.isMaster()) {
            Entity entity = driver.getEntity();
            ManagementContext managementContext = ((EntityInternal) driver.getEntity()).getManagementContext();

            String mpiVersion = entity.getConfig(SgeNode.MPI_VERSION);
            DownloadResolver mpiDownloadResolver = managementContext.getEntityDownloadsManager().newDownloader(
                    (EntityDriver) this, "mpi", ImmutableMap.of("addonversion", mpiVersion));
            List<String> mpiUrls = mpiDownloadResolver.getTargets();
            String mpiSaveAs = mpiDownloadResolver.getFilename();

            return ImmutableList.<String>builder()
                    .addAll(mpiUrls)
                    .add(format("tar xvzf %s", mpiSaveAs))
                    .build();
        } else {
            return ImmutableList.of();
        }
    }

    public List<String> customizeCommands() {
        return ImmutableList.of();
    }
}
