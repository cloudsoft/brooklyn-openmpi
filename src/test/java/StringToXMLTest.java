import brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zaid.mohsin on 12/03/2014.
 */
public class StringToXMLTest {

    private static final Logger log = LoggerFactory.getLogger(StringToXMLTest.class);

    private String xmlString = "<?xml version='1.0'?>\n" +
            "<qhost xmlns:xsd=\"http://gridscheduler.svn.sourceforge.net/viewvc/gridscheduler/trunk/source/dist/util/resources/schemas/qhost/qhost.xsd?revision=11\">\n" +
            " <host name='global'>\n" +
            "   <hostvalue name='arch_string'>-</hostvalue>\n" +
            "   <hostvalue name='num_proc'>-</hostvalue>\n" +
            "   <hostvalue name='load_avg'>-</hostvalue>\n" +
            "   <hostvalue name='mem_total'>-</hostvalue>\n" +
            "   <hostvalue name='mem_used'>-</hostvalue>\n" +
            "   <hostvalue name='swap_total'>-</hostvalue>\n" +
            "   <hostvalue name='swap_used'>-</hostvalue>\n" +
            " </host>\n" +
            " <host name='ip-10-250-37-116.us-west-2.compute.internal'>\n" +
            "   <hostvalue name='arch_string'>linux-x64</hostvalue>\n" +
            "   <hostvalue name='num_proc'>1</hostvalue>\n" +
            "   <hostvalue name='load_avg'>0.07</hostvalue>\n" +
            "   <hostvalue name='mem_total'>1.6G</hostvalue>\n" +
            "   <hostvalue name='mem_used'>125.1M</hostvalue>\n" +
            "   <hostvalue name='swap_total'>896.0M</hostvalue>\n" +
            "   <hostvalue name='swap_used'>0.0</hostvalue>\n" +
            " </host>\n" +
            "</qhost>" +
            "asdfasdfasdfadfadf";
    @Test(description = "testing the string to be converted to xml")
    public void testStringToXml() {



        xmlString = xmlString.substring(0,xmlString.lastIndexOf("</qhost>")+8);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;


        try {
            builder = factory.newDocumentBuilder();
            Document document = builder.parse(Streams.newInputStreamWithContents(xmlString));

            XPathFactory xpf = XPathFactory.newInstance();
            XPath xpath = xpf.newXPath();
            Node queueNode = (Node) xpath.evaluate("//host[contains(@name,'ip-10-250-37-116.us-west-2.compute.internal')]/hostvalue[@name='mem_total']", document, XPathConstants.NODE);


            log.warn(queueNode.getTextContent());
            String xpathout =  queueNode.getTextContent();
            Assert.assertEquals(xpathout, "1.6G");

        } catch (Exception e) {
            e.printStackTrace();
        }




    }

}
