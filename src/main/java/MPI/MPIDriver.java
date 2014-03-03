package MPI;

import brooklyn.entity.basic.SoftwareProcessDriver;
import brooklyn.entity.java.JavaSoftwareProcessDriver;
import java.lang.String;import java.util.List;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public interface MPIDriver extends JavaSoftwareProcessDriver {

    public void updateHosts(List<String> mpiHosts);
    public Integer getNumOfProcessors();
    public String getAdminHost();
    public String getExecHosts();
    public String getSubmissionHosts();
    public String getSgeRoot();
    public String getSgeAdmin();
    public String getArch();
}