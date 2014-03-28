package io.cloudsoft.hpc.sge;


import brooklyn.entity.basic.VanillaSoftwareProcessDriver;


public interface SgeDriver extends VanillaSoftwareProcessDriver {

    public Integer getNumOfProcessors();


    public String getSgeRoot();

    public String getSgeAdmin();

    public String getArch();

    public String getPeName();

    public void removeSlave(SgeNode slave);

    public void addSlave(SgeNode slave);

    public String getRunDir();


}