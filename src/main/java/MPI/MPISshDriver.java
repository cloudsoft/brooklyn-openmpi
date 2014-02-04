package MPI;

import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.basic.VanillaSoftwareProcessSshDriver;
import brooklyn.location.basic.SshMachineLocation;

import java.util.List;
import java.util.Map;

/**
 * Created by zaid.mohsin on 04/02/2014.
 */
public class MPISshDriver extends VanillaSoftwareProcessSshDriver implements MPIDriver {

    public MPISshDriver(EntityLocal entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public void install() {
        super.install();

        newScript(INSTALLING)
                .body.append("sudo apt-get update")
                .body.append("sudo apt-get -y install build-essential")
                .body.append("sudo apt-get -y install openmpi-bin openmpi-checkpoint openmpi-common openmpi-doc libopenmpi-dev")
                .execute();
    }

    @Override
    public void customize() {
        //super.customize();
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
        //super.launch();

        newScript(LAUNCHING)
                .body.append("pwd")
                .execute();
    }

    @Override
    public boolean isRunning() {

        entity.setAttribute(MPINode.SERVICE_UP,Boolean.TRUE);
        return true;
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void kill() {
        super.kill();
    }
    @Override
    public void updateHostnames(List<String> hostnamesList) {

    }

    @Override
    public void setAndFetchMasterSshKey() {
        newScript(LAUNCHING)
                .body.append("chmod 700 ~/.ssh")
                .body.append("echo \"StrictHostKeyChecking no\" >> /etc/ssh/ssh_config")
                .body.append("ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -C \"Open MPI\" -P \"\"")
                .body.append("cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
                .body.append("chmod 600 ~/.ssh/authorized_keys ")
                .execute();

        //fetch the id_rsa file from masternode
        getMachine().copyFrom("~/.ssh/id_rsa","~/Dev/ycsboutput/master_rsa");

    }
}
