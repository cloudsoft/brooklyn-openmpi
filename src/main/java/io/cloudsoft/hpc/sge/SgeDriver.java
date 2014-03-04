package io.cloudsoft.hpc.sge;

import brooklyn.entity.java.JavaSoftwareProcessDriver;
import java.lang.String;import java.util.List;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public interface SgeDriver extends JavaSoftwareProcessDriver {

    public void updateHosts(List<String> mpiHosts);
    public Integer getNumOfProcessors();
    public String getAdminHost();
    public String getExecHosts();
    public String getSubmissionHosts();
    public String getSgeRoot();
    public String getSgeAdmin();
    public String getArch();

    public void removeSlave(String alias);
    public void addSlave(String alias);
}