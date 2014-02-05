package MPI;

import brooklyn.entity.basic.SoftwareProcessDriver;

import java.lang.String;import java.util.List;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public interface MPIDriver extends SoftwareProcessDriver {

    public void updateHostsFile();
}