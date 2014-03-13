package io.cloudsoft.hpc.sge;

import brooklyn.entity.java.JavaSoftwareProcessDriver;
import java.lang.String;import java.util.List;
import java.util.Map;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public interface SgeDriver extends JavaSoftwareProcessDriver {

    public Integer getNumOfProcessors();
    public String getAdminHosts();
    public String getExecHosts();
    public String getSubmissionHosts();
    public String getSgeRoot();
    public String getSgeAdmin();
    public String getArch();
    public String getPeName();

    public void removeSlave(SgeNode slave);
    public void addSlave(SgeNode slave);
    public void updatePE(String peName, Integer numOfProcessors);
    public String getRunDir();

}