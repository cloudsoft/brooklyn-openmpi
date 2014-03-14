package io.cloudsoft.hpc.sge;

import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.event.feed.ssh.SshFeed;
import brooklyn.event.feed.ssh.SshPollConfig;
import brooklyn.event.feed.ssh.SshPollValue;
import brooklyn.event.feed.ssh.SshValueFunctions;
import brooklyn.location.Location;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.stream.Streams;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class SgeNodeImpl extends SoftwareProcessImpl implements SgeNode {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SgeNodeImpl.class);
    //Factories for building the xml output and using it in the feed
    private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    private static XPathFactory xpf = XPathFactory.newInstance();
    private AtomicBoolean setNoOfProcessors;
    private transient SshFeed sshFeed;

    @VisibleForTesting
    static Function<SshPollValue, String> qstatInfoFunction(final String queueName, final String alias, final String field) {
        return Functions.compose(new Function<String, String>() {
            @Override
            public String apply(@Nullable String input) {

                //remote trailing executed script status to parse the xml output correctly
                String xmlString = input.substring(0, input.lastIndexOf("</job_info>") + 11);

                //FIXME use aliases instead of subnethostnames and add the queue name to the query.


                try {
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    Document document = builder.parse(Streams.newInputStreamWithContents(xmlString));

                    //query the xml for the requested field.
                    XPath xpath = xpf.newXPath();
                    Node queueNode = (Node) xpath.evaluate(format("//Queue-List[contains(name,'%s')]/%s", alias, field), document, XPathConstants.NODE);

                    return queueNode.getTextContent();

                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }

            }
        }, SshValueFunctions.stdout());
    }


    private static Function<SshPollValue, String> qhostInfoFunction(final String queueName, final String alias, final String field) {
        return Functions.compose(new Function<String, String>() {
            @Override
            public String apply(@Nullable String input) {

                //remote trailling executed script status to parse the xml output correctly
                String xmlString = input.substring(0, input.lastIndexOf("</qhost>") + 8);

                //FIXME use aliases instead of subnethostnames and add the queue name to the query.


                try {
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    Document document = builder.parse(Streams.newInputStreamWithContents(xmlString));

                    XPath xpath = xpf.newXPath();
                    Node queueNode = (Node) xpath.evaluate(format("//host[contains(@name,'%s')]/hostvalue[@name='%s']", alias, field), document, XPathConstants.NODE);

                    return queueNode.getTextContent();

                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }


            }
        }, SshValueFunctions.stdout());
    }

    @Override
    public void init() {
        super.init();

    }

    @Override
    protected void doStart(Collection<? extends Location> locations) {
        super.doStart(locations);
    }

    @Override
    protected void doStop() {
        SgeNode master = getAttribute(SGE_MASTER);

        if (!isMaster()) {
            Entities.invokeEffectorWithArgs(this, master, SgeNode.REMOVE_SLAVE, getAttribute(SgeNode.HOSTNAME));

        }
        super.doStop();
    }

    @Override
    public void addSlave(SgeNode slave) {
        if (isMaster()) {
            getDriver().addSlave(slave);
        } else {
            throw new UnsupportedOperationException("Unsupported Operation on Slave Nodes");
        }
    }

    @Override
    public void removeSlave(SgeNode slave) {
        if (isMaster()) {
            getDriver().removeSlave(slave);
        } else {
            throw new UnsupportedOperationException("Unsupported Operation on Slave Nodes");
        }
    }

    @Override
    public String getClusterName() {
        return getConfig(SgeNode.SGE_CLUSTER_NAME);
    }

    @Override
    public void updatePE(String peName, Integer numOfProcessors) {
        getDriver().updatePE(peName, numOfProcessors);
    }

    @Override
    public String getPEname() {
        return getConfig(SgeNode.SGE_PE_NAME);
    }


    @Override
    public String getSgeRoot() {
        return getConfig(SGE_ROOT);
    }

    @Override
    public void connectSensors() {
        super.connectSensors();
        connectServiceUpIsRunning();

        // Find an SshMachineLocation for the UPTIME feed

        Optional<Location> location = Iterables.tryFind(getLocations(), Predicates.instanceOf(SshMachineLocation.class));
        if (!location.isPresent())
            throw new IllegalStateException("Could not find SshMachineLocation in list of locations");
        SshMachineLocation machine = (SshMachineLocation) location.get();

        String qstatCmd = format("source %s/sge_profile.conf;qstat -f -xml", getDriver().getRunDir());
        String qhostCmd = format("source %s/sge_profile.conf;qhost -xml", getDriver().getRunDir());

        String alias = getAttribute(SgeNode.SGE_NODE_ALIAS);

        sshFeed.builder()
                .entity(this)
                .machine(machine)
                .period(5, TimeUnit.SECONDS)
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_LOAD_AVG)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "load_avg")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_QTYPE)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "qtype")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_SLOTS_USED)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "slots_used")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_SLOTS_RESERVED)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "slots_resv")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_SLOTS_TOTAL)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "slots_total")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_NAME)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "name")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QSTAT_ARCH)
                        .command(qstatCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qstatInfoFunction("all.q", alias, "arch")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_LOAD_AVG)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "load_avg")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_NUM_PROC)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "num_proc")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_MEM_TOTAL)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "mem_total")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_MEM_USED)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "mem_used")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_SWAP_TOTAL)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "swap_total")))
                .poll(new SshPollConfig<String>(SgeNode.SGE_QHOST_SWAP_USED)
                        .command(qhostCmd)
                        .onFailureOrException(Functions.constant("error"))
                        .onSuccess(qhostInfoFunction("all.q", alias, "swap_used")))

                .build();


    }

    @Override
    public void disconnectSensors() {
        super.disconnectSensors();
        disconnectServiceUpIsRunning();

        if (sshFeed != null) sshFeed.stop();
    }

    @Override
    public void updateHosts(List<String> sgeHosts) {

        log.info("SGE hosts on SgeNodeImpl are {}", sgeHosts.toString());

        setAttribute(SGE_HOSTS, sgeHosts);

    }

    @Override
    public Boolean isMaster() {
        return (Boolean.TRUE.equals(getAttribute(MASTER_FLAG)));
    }

    @Override
    public Class<? extends SgeDriver> getDriverInterface() {
        return SgeDriver.class;
    }

    @Override
    public SgeDriver getDriver() {
        return (SgeDriver) super.getDriver();
    }


}
