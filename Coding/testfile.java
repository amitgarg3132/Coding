package com.walmartlabs.services.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.walmartlabs.services.util.base.TransformationUtils;
import com.walmartlabs.services.util.base.XMLUtils;
import com.yantra.yfc.log.YFCLogLevel;
import com.yantra.yfc.log.YFCLogManager;
import com.yantra.yfs.japi.YFSEnvironment;
import com.yantra.yfs.japi.YFSException;

public class CreateEventPayloadTest {

	private Document inputCreateOrder = null;
	private Document inputCreateFulfillmentOrder = null;
	private Document inputOrderHoldChange = null;
	private Document inputOrderLineHoldChange = null;
	private Document inputOrderCancel = null;
	private Document inputOrderNotes = null;
	private Document inputPOShipped = null;
	private Document inputAckFulfillmentOrder = null;
	private Document inputPOPickComplete=null;

	private String outputCreateOrder = null;
	private String outputCreateFulfillmentOrder = null;
	private String outputOrderHoldChange = null;
	private String outputOrderLineHoldChange = null;
	private String outputOrderCancel = null;
	private String outputOrderNotes = null;
	private String outputPOShipped = null;
	private String outputAckFulfillmentOrder = null;
	private String outputPOPickComplete=null;
	YFSEnvironment env = null;


	@Before
	public void setUp() throws ParserConfigurationException, TransformerException, IOException {
		inputCreateOrder = readXMLDocument("ReqEventBusCreateOrder.xml");
		inputCreateFulfillmentOrder = readXMLDocument("ReqEventBusCreateFulfillmentOrder.xml");
		inputAckFulfillmentOrder = readXMLDocument("ReqEventBusAckFulfillmentOrder.xml");
		inputOrderHoldChange = readXMLDocument("ReqEventBusHoldChangeOrder.xml");
		inputOrderLineHoldChange = readXMLDocument("ReqEventBusHoldChangeOrderLine.xml");
		inputOrderCancel = readXMLDocument("ReqEventBusOrderCancel.xml");
		inputOrderNotes = readXMLDocument("ReqEventBusAddNoteHeader.xml");
		inputPOShipped = readXMLDocument("ReqEventBusFulfillmentOrderShipped.xml");
		inputPOPickComplete = readXMLDocument("ReqEventBusFulfillmentOrderPickComplete.xml");

		outputCreateOrder = this.readFile("RespEventBusCreateOrder.json");
		outputCreateFulfillmentOrder = this.readFile("RespEventBusCreateFulfillmentOrder.json");
		outputAckFulfillmentOrder = this.readFile("RespEventBusAckFulfillmentOrder.json");
		outputOrderHoldChange = this.readFile("RespEventBusHoldChangeOrder.json");
		outputOrderLineHoldChange = this.readFile("RespEventBusHoldChangeOrderLine.json");
		outputOrderCancel = this.readFile("RespEventBusOrderCancel.json");
		outputOrderNotes = this.readFile("RespEventBusAddNoteHeader.json");
		outputPOShipped = this.readFile("RespEventBusFulfillmentOrderShipped.json");
		outputPOPickComplete=this.readFile("RespEventBusFulfillmentOrderPickComplete.json");
		env= mock(YFSEnvironment.class);
	}

	@Test
	public void testCreateOrderEvent() throws Exception {
		testEvent(inputCreateOrder, outputCreateOrder);
	}

	@Test
	public void testCreateFulfillmentOrderEvent() throws Exception {
		testEvent(inputCreateFulfillmentOrder, outputCreateFulfillmentOrder);
	}

	@Test
	public void testAckFulfillmentOrderEvent() throws Exception {
		testEvent(inputAckFulfillmentOrder, outputAckFulfillmentOrder);
	}

	@Test
	public void testOrderHoldChangeEvent() throws Exception {
		testEvent(inputOrderHoldChange, outputOrderHoldChange);
	}

	@Test
	public void testOrderLineHoldChangeEvent() throws Exception {
		testEvent(inputOrderLineHoldChange, outputOrderLineHoldChange);
	}

	@Test
	public void testOrderCancelEvent() throws Exception {
		testEvent(inputOrderCancel, outputOrderCancel);
	}

	@Test
	public void testOrderNotesEvent() throws Exception {
		testEvent(inputOrderNotes, outputOrderNotes);
	}

	@Test
	public void testPOShippedEvent() throws Exception {
		testEvent(inputPOShipped, outputPOShipped);			
	}

	@Test
	public void testPOPickCompleteEvent() throws Exception {
		testEvent(inputPOPickComplete, outputPOPickComplete);
	}
	@Test
	public void testEvents() throws Exception {
		try
		{
			testCreateOrderEvent();
			testCreateFulfillmentOrderEvent();
			testAckFulfillmentOrderEvent();
			testOrderHoldChangeEvent();
			testOrderLineHoldChangeEvent();
			testOrderNotesEvent();
			testOrderCancelEvent();
			testPOShippedEvent();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			fail("Exception while testing the class :testPublishEvent");
		}
	}

	private void testEvent(Document input, String output) throws YFSException, SecurityException, IllegalArgumentException, NoSuchFieldException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException, XPathExpressionException {
		YFCLogManager.enableTransactionTracing(YFCLogLevel.DEBUG.toString());
		CreateEventPayload createEventPayload	= new CreateEventPayload();
		Document docOut = createEventPayload.publishEvent(env, input);

		Element elePayload = docOut.getDocumentElement();
		
		String eventMsg = TransformationUtils.getCharacterDataFromElement(elePayload);	

		JSONObject jEvent = null;
		if(!XMLUtils.isVoid(eventMsg))
		{
			jEvent = new JSONObject(eventMsg);	
			jEvent.getJSONObject("header").remove("timestamp");
			jEvent.getJSONObject("header").remove("eventID");

		}

		assertEquals("validating the output xml: ",jEvent.toString(),output);
	}

	/**
	 * Method written to read the XML document from the input file
	 * 
	 * @param fileName
	 * <br/>
	 *            - Name of the file to be read
	 * @return Document <br/>
	 * <br/>
	 *         - The XML Document that is read from the file
	 * @throws ParserConfigurationException
	 *             , IOException, SAXException <br/>
	 *             - The exceptions thrown by this method
	 */
	private Document readXMLDocument(String fileName) {

		Document inputDocument = null;
		InputStream isInputDocument = this.getClass().getResourceAsStream(fileName);
		try {
			inputDocument = XMLUtils.getDocument(isInputDocument);
		} catch (ParserConfigurationException e) {
			System.out.println("ParserConfigurationException happened");
		} catch (IOException e) {
			System.out.println("IOException happened");
		} catch (SAXException e) {
			System.out.println("SAXException happened");
		}
		return inputDocument;
	}

	String readFile(String fileName) throws IOException{
		InputStream in = this.getClass().getResourceAsStream(fileName);
		StringBuilder sb=new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String read;

		while((read=br.readLine()) != null) {
			sb.append(read);   
		}

		br.close();
		return sb.toString();	
	}

}
