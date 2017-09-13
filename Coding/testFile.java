package com.walmartlabs.services.events;
 
import com.google.common.base.Preconditions;
import com.walmart.commons.utils.CollectionUtils;
import com.walmart.services.common.model.OfferId;
import com.walmart.services.common.model.measurement.Measurement;
import com.walmart.services.common.model.measurement.UnitOfMeasureEnum;
import com.walmart.services.common.model.money.CurrencyUnitEnum;
import com.walmart.services.common.model.money.MoneyType;
import com.walmart.services.order.common.enums.ChargeCategory;
import com.walmart.services.order.common.enums.DateTypeId;
import com.walmart.services.order.common.model.Charge;
import com.walmart.services.order.common.model.ContactInfo;
import com.walmart.services.order.common.model.DeliveryReservationDetail;
import com.walmart.services.order.common.model.OrderDate;
import com.walmart.services.order.common.model.PoLineStatusInfo;
import com.walmart.services.order.common.model.PurchaseOrder;
import com.walmart.services.order.common.model.PurchaseOrderLine;
import com.walmart.services.order.common.model.PurchaseOrderShipment;
import com.walmartlabs.services.api.shipment.PublishASNDetails;
import com.walmartlabs.services.util.base.DateUtils;
import com.walmartlabs.services.util.base.LabsCommonUtil;
import com.walmartlabs.services.util.base.LogUtils;
import com.walmartlabs.services.util.base.Logger;
import com.walmartlabs.services.util.base.StringUtils;
import com.walmartlabs.services.util.base.XMLUtils;
import com.walmartlabs.services.util.base.LabsConstants;
import com.walmartlabs.services.util.base.LabsDozerMapper;
import com.walmartlabs.services.util.base.LabsHandler;
import com.walmartlabs.services.util.jaxb.order.Order;
import com.walmartlabs.services.util.jaxb.order.OrderHoldType;
import com.walmartlabs.services.util.jaxb.order.OrderInvoice;
import com.walmartlabs.services.util.jaxb.order.OrderLine;
import com.walmartlabs.services.util.jaxb.shipmentlist.Shipment;
import com.yantra.yfc.util.YFCCommon;
import com.yantra.yfs.japi.YFSEnvironment;
import com.yantra.yfs.japi.YFSException;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.dozer.DozerBeanMapper;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathException;
import javax.xml.xpath.XPathExpressionException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 
 * @author kkambha
 * 
 */
public class CreateEventPayload {

	private static Logger logger = LogUtils
			.getInstance(CreateEventPayload.class);

	private String EVENT = "";

	private String VERTICALID = "";

	private String TENANTID = "";

	private String SCHEMAID = "";

	private UUID eventID = LabsCommonUtil.getRandomUUID();

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @return
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws NoSuchMethodException
	 * @throws NoSuchFieldException
	 * @throws IllegalArgumentException
	 * @throws SecurityException
	 * @throws XPathExpressionException
	 * @throws TransformerException
	 * @throws XPathException
	 * @throws SAXException
	 * @throws IOException
	 * @throws ParserConfigurationException
	 * @throws DOMException
	 * @throws JsonMappingException
	 * @throws JSONException
	 */
	public Document publishEvent(YFSEnvironment env, Document inputDocument)
			throws YFSException, SecurityException, IllegalArgumentException,
			NoSuchFieldException, NoSuchMethodException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException, XPathExpressionException {
		String strMethodName = "CreateEventPayload.publishEvent()";

		logger.beginTimer("Starting " + strMethodName);
		logger.debug("Input Document to", strMethodName, inputDocument);
		String sEventMsg = null;

		logger.debug("Event XML:", inputDocument);

		Element eleInput = inputDocument.getDocumentElement();
		EVENT = eleInput.getAttribute(EventBusConstants.A_EVENT);
		String auditTrxId = eleInput.getAttribute("AuditTransactionId");

		if (XMLUtils.isVoid(EVENT)) {
			logger.debug("Event is blank");
			throw LabsHandler.handleException(
					LabsConstants.ERROR_MANDATORY_PARAMS_MISSING,
					"Event value for EventBus is mandatory" + strMethodName);
		}

		logger.debug("Event created:" + EVENT);

		List<String> alValues = fetchEnumDateTypeIds("WALMART.COM", env);
		parseAndremoveDateTypeId(inputDocument, alValues);

		Document payload;
		/**
		 * Form PayLoad based on the event type
		 */
		try {
			if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_CREATED)
							.toString()))
				sEventMsg = orderCreate(inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CREATED)
							.toString()))
				sEventMsg = orderFulfillmentCreate(env, inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_ACKNOWLEDGED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_ACKNOWLEDGED;
				sEventMsg = orderFulfillmentAck(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_HDRHOLDCHANGE)
							.toString()))
				sEventMsg = orderHoldChange(inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_LINEHOLDCHANGE)
							.toString()))
				sEventMsg = orderLineHoldChange(inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_CANCEL)
							.toString()))
				sEventMsg = orderCancel(inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_NOTES)
							.toString()))
				sEventMsg = orderNotes(inputDocument);
			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_SHIPPED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_SHIPPED;
				sEventMsg = poShipped(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ADV_FO_NOTIFICATION)
							.toString())) {
				// AD PFO Shipped - this will be conditional
				SCHEMAID = EventBusConstants.SCHEMA_ADV_FO_NOTIFICATION;
				sEventMsg = associateDeliveryPFOShipped(env, inputDocument);
			}

			else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_DELIVERED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_DELIVERED;
				sEventMsg = poShipped(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_PICK_COMPLETE)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_PICK_COMPLETE;
				sEventMsg = poPickComplete(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CANCEL_BACKORDERED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_CANCEL_BACKORDERED;
				sEventMsg = poCancel(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_PICK_IN_PROGRESS)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_PICK_IN_PROGRESS;
				sEventMsg = poPickInProgress(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_ARRIVED_AT_STORE)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_ARRIVED_AT_STORE;
				sEventMsg = poArrivedAtStore(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_BINNING_COMPLETE)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_BINNING_COMPLETE;
				sEventMsg = poShipped(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_PICKED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_PICKED;
				sEventMsg = poLine(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CONSOLIDATED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_CONSOLIDATED;
				sEventMsg = poLine(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_SHIPPING_LABEL_GENERATED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_SHIPPING_LABEL_GENERATED;
				sEventMsg = poLine(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_LOADED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_LOADED;
				sEventMsg = poLine(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CANCEL_DISCONTINUED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_CANCEL_DISCONTINUED;
				sEventMsg = poCancel(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CANCEL_UNRECOGNIZED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_CANCEL_UNRECOGNIZED;
				sEventMsg = poCancel(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_UNRECOGNIZED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_UNRECOGNIZED;
				sEventMsg = poPickInProgress(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_FILLED_OR_KILLED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_ORDER_FILLED_OR_KILLED;
				sEventMsg = orderFilledOrKilledEvent(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ASSOCIATE_DELIVERY)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_ASSOCIATE_DELIVERY;
				sEventMsg = associateDelivery(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.SHIPMENT_INVOICED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_SHIPMENT_INVOICED;
				sEventMsg = orderInvoiced(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_AMENDED)
							.toString())
					&& "AMMEND_ORDER".equalsIgnoreCase(auditTrxId)) {
				SCHEMAID = EventBusConstants.SCHEMA_ORDER_AMENDED;
				sEventMsg = orderAmended(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.FOCI_HOLD)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_ORDER_FOCI_HOLD;
				sEventMsg = orderFOCIHold(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_CUSTOMER_PICKED)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_CUSTOMER_PICKED;
				sEventMsg = poPickComplete(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.ORDER_REFUND)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_ORDER_REFUND;
				sEventMsg = orderRefund(inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_SHIPPED_DELIVERY_DATE_UPDATE)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_SHIPPED_DELIVERY_DATE_UPDATE;
				sEventMsg = poShipped(env, inputDocument);
			} else if (EVENT
					.equalsIgnoreCase((EventBusConstants.EventEnum.PO_SHIPMENT_TD_EDD_UPDATE)
							.toString())) {
				SCHEMAID = EventBusConstants.SCHEMA_PO_SHIPMENT_TD_EDD_UPDATE;
				sEventMsg = poShipped(env, inputDocument);
			} else {
				logger.debug("Ignoring as the event is not valid");
				return inputDocument;
			}

			sEventMsg = appendHeaders(sEventMsg);
			payload = LabsCommonUtil.wrapJSONInXML(env, sEventMsg);

		} catch (JSONException e) {
			throw LabsHandler.handleException("JSONException",
					"JSONException in CreateEventPayload.publishEvent", e);
		} catch (JsonGenerationException e) {
			throw LabsHandler
					.handleException(
							"JsonGenerationException",
							"JsonGenerationException in CreateEventPayload.publishEvent",
							e);
		} catch (JsonMappingException e) {
			throw LabsHandler.handleException("JsonMappingException",
					"JsonMappingException in CreateEventPayload.publishEvent",
					e);
		} catch (DOMException e) {
			throw LabsHandler.handleException("DOMException",
					"DOMException in CreateEventPayload.publishEvent", e);
		} catch (ParserConfigurationException e) {
			throw LabsHandler
					.handleException(
							"ParserConfigurationException",
							"ParserConfigurationException in CreateEventPayload.publishEvent",
							e);
		} catch (IOException e) {
			throw LabsHandler.handleException("IOException",
					"IOException in CreateEventPayload.publishEvent", e);
		} catch (SAXException e) {
			throw LabsHandler.handleException("SAXException",
					"SAXException in CreateEventPayload.publishEvent", e);
		} catch (XPathException e) {
			throw LabsHandler.handleException("XPathException",
					"XPathException in CreateEventPayload.publishEvent", e);
		} catch (TransformerException e) {
			throw LabsHandler.handleException("TransformerException",
					"TransformerException in CreateEventPayload.publishEvent",
					e);
		}

		logger.debug("Event JSON", payload);

		logger.endTimer("Exiting Execution of" + strMethodName);
		return payload;
	}

	/**
	 * Generate Associate Delivery PFO message to be published to CP Kafka
	 * 
	 * @param inputDocument
	 * @return
	 * @throws XPathException
	 * @throws ParserConfigurationException
	 * @throws JSONException
	 */
	private String associateDeliveryPFOShipped(YFSEnvironment env,
			Document inputDocument) throws XPathException,
			ParserConfigurationException, JSONException, TransformerException,
			RemoteException {
		String sMethodName = "associateDeliveryPFOShipped";
		logger.beginTimer(sMethodName);
		logger.debug(
				"associateDeliveryPFOShipped The input to the method is : ",
				inputDocument);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_SHIPMENT_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_SHIPMENT_ORDER);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_FULFILLMENTORDER_AD);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ sMethodName);

		com.walmart.services.order.common.model.Order orderObject = new com.walmart.services.order.common.model.Order();
		final List<PurchaseOrder> purchaseOrders = new ArrayList<PurchaseOrder>();
		final List<com.walmart.services.order.common.model.OrderLine> orderLines = new ArrayList<com.walmart.services.order.common.model.OrderLine>();

		Element shipmentEle = inputDocument.getDocumentElement();
		List<Element> shipmentLines = XMLUtils.getElementListByTagName(
				shipmentEle, "ShipmentLine");

		// Set orderNo
		orderObject.setOrderNo(shipmentLines.get(0)
				.getAttribute("CustomerPoNo"));

		// Set deliveryReservationDetails
		List<DeliveryReservationDetail> deliveryReservationDetails = new ArrayList<DeliveryReservationDetail>();
		DeliveryReservationDetail deliveryReservationDetail = new DeliveryReservationDetail();
		deliveryReservationDetail.setDispenseType("Delivery");
		deliveryReservationDetails.add(deliveryReservationDetail);
		orderObject.setDeliveryReservationDetails(deliveryReservationDetails);

		// Prepare ShipToAddress
		Element eleToAddressFromShipment = XMLUtils.getFirstElementByName(
				shipmentEle, "ToAddress");
		ContactInfo shipToAddress = null;
		if (eleToAddressFromShipment != null) {
			shipToAddress = (ContactInfo) LabsDozerMapper
					.generateMappedObject(
							com.walmartlabs.services.util.jaxb.shipmentlist.ToAddress.class,
							ContactInfo.class,
							XMLUtils.getDocumentForElement(eleToAddressFromShipment),
							dbMapper);
		}

		// Prepare PurchaseOrder
		PurchaseOrder po = new PurchaseOrder();

		// Prepare Purchase order shipment
		// Check if shipVia is null or empty
		String shipVia = shipmentEle.getAttribute("ShipVia");
		if (shipVia.equals(" ") || StringUtils.isNullOrEmpty(shipVia)) {
			logger.debug("ShipVia attribute is null or empty!");
			shipmentEle.setAttribute("ShipVia", "");
		}

		PurchaseOrderShipment purchaseOrderShipment = (PurchaseOrderShipment) LabsDozerMapper
				.generateMappedObject(
						Shipment.class,
						com.walmart.services.order.common.model.PurchaseOrderShipment.class,
						inputDocument, dbMapper);
		if (shipToAddress != null) {
			po.setShipToAddress(shipToAddress);
		}
		logger.debug("PurchaseOrderShipment in JSON created!");

		String orderNo = null;
		Element orderElement = null;

		for (int i = 0; i < shipmentLines.size(); i++) { // Ideally it would be
															// only one
															// ShipmentLine

			Element shipmentLine = shipmentLines.get(i);

			if (shipmentLine != null) {
				// Get Order List
				final String sOrderNo = shipmentLine
						.getAttribute(LabsConstants.A_CUSTOMER_Po_NO);
				final String sEnterpriseCode = inputDocument
						.getDocumentElement().getAttribute(
								LabsConstants.A_ENTERPRISE_CODE);

				PublishASNDetails publishASNDetails = new PublishASNDetails();
				orderElement = publishASNDetails.getSalesOrderDetails(env,
						sOrderNo, sEnterpriseCode, null);

				// Generate OrderLines
				orderNo = shipmentLine.getAttribute("OrderNo");
				NodeList orderLineList = shipmentLine
						.getElementsByTagName("OrderLine");
				int numberOfOrderLines = orderLineList.getLength();
				for (int j = 0; j < numberOfOrderLines; j++) {

					Element orderLine = (Element) orderLineList.item(j);
					// Element orderLine =
					// XMLUtils.getFirstElementByName(shipmentLine,
					// LabsConstants.E_ORDER_LINE);

					if (orderLine != null) {
						stampCharges(inputDocument, orderLine, shipmentLine,
								orderElement);

						com.walmart.services.order.common.model.OrderLine orderLineJson = (com.walmart.services.order.common.model.OrderLine) LabsDozerMapper
								.generateMappedObject(
										OrderLine.class,
										com.walmart.services.order.common.model.OrderLine.class,
										XMLUtils.getDocumentForElement(orderLine),
										dbMapper);

						// OrderLine ShipToAddress should be present
						if (orderLineJson.getShipToAddress() == null
								|| orderLineJson.getShipToAddress()
										.getStoreFrontId() == null) {
							ContactInfo contactInfo = new ContactInfo();
							contactInfo.setStoreFrontId(purchaseOrderShipment
									.getStoreFrontId());
							orderLineJson.setShipToAddress(contactInfo);
						}

						// If charges are null, default them
						List<Charge> charges = new ArrayList<Charge>();
						Charge charge = new Charge();
						charge.setChargeCategory(ChargeCategory.PRODUCT);
						Measurement chargeQuantity = new Measurement();
						chargeQuantity.setMeasurementValue(BigDecimal.ONE);
						chargeQuantity.setUnitOfMeasure(UnitOfMeasureEnum.EACH);
						charge.setChargeQuantity(chargeQuantity);
						charge.setIsDiscount(Boolean.FALSE);
						charge.setIsBillable(Boolean.TRUE);

						MoneyType chargePerUnit = new MoneyType(orderLineJson
								.getUnitPrice().getCurrencyAmount(),
								CurrencyUnitEnum.USD);
						charge.setChargePerUnit(chargePerUnit);

						charges.add(charge);
						orderLineJson.setCharges(charges);

						orderLines.add(orderLineJson);

						// Generate Payment methods
						if (orderElement != null) {
							com.walmart.services.order.common.model.Order orderJson = (com.walmart.services.order.common.model.Order) LabsDozerMapper
									.generateMappedObject(
											Order.class,
											com.walmart.services.order.common.model.Order.class,
											XMLUtils.getDocumentForElement(orderElement),
											dbMapper);
							if (orderJson != null
									&& !CollectionUtils.isEmpty(orderJson
											.getPaymentMethods()))
								orderObject.setPaymentMethods(orderJson
										.getPaymentMethods());
						}
					}
				}
				// Generate pickup persons
				// May not set anything, it will be all blank!
			}
		}

		// Generate purchaseOrders and purchaseOrderShipments
		po.setPurchaseOrderNo(orderNo);
		po.setShipNode(shipmentEle.getAttribute("ShipNode"));

		// Set po.TCNumber
		if (orderElement != null) {
			Element orderExtn = XMLUtils.getFirstElementByName(orderElement,
					LabsConstants.A_EXTN);
			if (orderExtn != null) {
				String tcNumber = orderExtn
						.getAttribute(LabsConstants.A_EXTN_TC_NO);
				po.setTcNumber(tcNumber);
			}
		}

		// set purchaseOrder.poDate -
		List<OrderDate> orderDates = new ArrayList<OrderDate>();
		OrderDate orderDate = new OrderDate();
		orderDate.setDateTypeId(DateTypeId.DELIVERY);

		List<OrderDate> shipmentDates = purchaseOrderShipment
				.getShipmentDates();
		for (OrderDate shipmentDate : shipmentDates) {
			if (shipmentDate.getDateTypeId().equals(DateTypeId.DELIVERY)) {
				orderDate.setExpectedDate(shipmentDate.getExpectedDate());
				break;
			}
		}
		orderDates.add(orderDate);
		po.setPoDate(orderDates);

		final List<PurchaseOrderShipment> purchaseOrderShipments = new ArrayList<PurchaseOrderShipment>();
		purchaseOrderShipments.add(purchaseOrderShipment);
		po.setPurchaseOrderShipments(purchaseOrderShipments);

		purchaseOrders.add(po);

		orderObject.setOrderLines(orderLines);
		orderObject.setPurchaseOrders(purchaseOrders);

		logger.debug("The AD orderObject is ", orderObject.toString());
		logger.endTimer(sMethodName);
		return orderObject.toString();
	}

	private void stampCharges(Document inputDocument, Element orderLine,
			Element shipmentLine, Element orderElement) throws RemoteException,
			ParserConfigurationException, XPathException, TransformerException {

		final String sMethodName = "associateDeliveryPFOShipped.stampCharges()";
		logger.beginTimer(sMethodName);

		final String sOLKey = orderLine
				.getAttribute(LabsConstants.A_CHAINED_FROM_OLK);

		Element soLElement = XMLUtils.getElementFromXPath(orderElement,
				"OrderLines/OrderLine[@OrderLineKey = '" + sOLKey + "']");

		if (YFCCommon.isVoid(soLElement)) {

			logger.debug("So Element is null for the OLK, so fetching using ItemID");

			String strItemId = shipmentLine.getAttribute("ItemID");

			soLElement = XMLUtils.getElementFromXPath(orderElement,
					"OrderLines/OrderLine[Item/@ItemID = '" + strItemId + "']");
		}

		// Adding the UPCode,ItemDesc, from the PO order line
		if (soLElement != null) {
			final Element itemElement = XMLUtils.getChildElement(soLElement,
					LabsConstants.E_ITEM);
			if (itemElement.getAttribute(LabsConstants.A_UPC_CODE) != null)
				shipmentLine.setAttribute(LabsConstants.A_UPC_CODE,
						itemElement.getAttribute(LabsConstants.A_UPC_CODE));
			if (itemElement.getAttribute(LabsConstants.A_ITEM_DESC) != null)
				shipmentLine.setAttribute(LabsConstants.A_ITEM_DESC,
						itemElement.getAttribute(LabsConstants.A_ITEM_DESC));
			if (shipmentLine.getAttribute(LabsConstants.A_QUANTITY) != null)
				shipmentLine.setAttribute(LabsConstants.A_QUANTITY,
						shipmentLine.getAttribute(LabsConstants.A_QUANTITY));

			// Adding the UnitPrice from SO
			Element priceInfo = XMLUtils.getChildElement(soLElement,
					LabsConstants.E_LINE_PRICE_INFO);
			if (priceInfo != null
					&& priceInfo.getAttribute(LabsConstants.A_UNIT_PRICE) != null) {
				shipmentLine.setAttribute(LabsConstants.A_UNIT_PRICE,
						priceInfo.getAttribute(LabsConstants.A_UNIT_PRICE));
				Element linePriceInfo = XMLUtils.appendChild(inputDocument,
						orderLine, LabsConstants.E_LINE_PRICE_INFO, null);
				linePriceInfo.setAttribute(LabsConstants.A_UNIT_PRICE,
						priceInfo.getAttribute(LabsConstants.A_UNIT_PRICE));
			}
			// Fetching all the LineTaxes on the OrderLine
			List<Element> nlist = XMLUtils.getElementsByTagName(soLElement,
					LabsConstants.E_LINE_TAX);
			stampLineTaxes(nlist, inputDocument, shipmentLine);

			// Fetching all the LineCharges on the OrderLine
			nlist = XMLUtils.getElementsByTagName(soLElement,
					LabsConstants.E_LINE_CHARGE);
			stampLineCharges(nlist, inputDocument, shipmentLine);
		}
		logger.debug("The updated inputDocument is : ", inputDocument);
		logger.endTimer(sMethodName);
	}

	/**
	 * This method will stamp line taxes in the POS and locker message
	 * 
	 * @param nlist
	 * @param asnDocument
	 * @param orderLineElement
	 */
	private void stampLineTaxes(final List<Element> nlist,
			final Document asnDocument, final Element orderLineElement) {
		final String sMethodName = "associateDeliveryPFOShipped.stampLineTaxes()";
		logger.beginTimer(sMethodName);
		Element lineTaxesElement = null;
		final int iTaxSize = nlist.size();
		if (iTaxSize > 0) {
			// Creating the Tax Elements
			lineTaxesElement = XMLUtils.appendChild(asnDocument,
					orderLineElement, LabsConstants.E_LINE_TAXES, null);

			for (final Element element : nlist) {
				final Element lineTaxElement = XMLUtils.appendChild(
						asnDocument, lineTaxesElement,
						LabsConstants.E_LINE_TAX, null);
				lineTaxElement.setAttribute(LabsConstants.TAX_NAME,
						element.getAttribute(LabsConstants.TAX_NAME));
				lineTaxElement.setAttribute(LabsConstants.A_REFERENCE_1,
						element.getAttribute(LabsConstants.A_REFERENCE_1));
				lineTaxElement.setAttribute(LabsConstants.A_CHARGE_CATEGORY,
						element.getAttribute(LabsConstants.A_CHARGE_CATEGORY));
				lineTaxElement.setAttribute(LabsConstants.A_CHARGE_NAME,
						element.getAttribute(LabsConstants.A_CHARGE_NAME));
			}
		}
		logger.endTimer(sMethodName);

	}

	/**
	 * This method will stamp line charges in the POS and locker message
	 * 
	 * @param nlist
	 * @param asnDocument
	 * @param orderLineElement
	 */
	private void stampLineCharges(final List<Element> nlist,
			final Document asnDocument, final Element orderLineElement) {
		final String sMethodName = "associateDeliveryPFOShipped.stampLineCharges()";
		logger.beginTimer(sMethodName);
		Element lineChargesElement = null;
		int iChargeSize = nlist.size();
		iChargeSize = nlist.size();
		if (iChargeSize > 0) {
			// Creating the Charge Elements
			lineChargesElement = XMLUtils.appendChild(asnDocument,
					orderLineElement, LabsConstants.E_LINE_CHARGES, null);

			for (final Element element : nlist) {
				final Element lineChargeElement = XMLUtils.appendChild(
						asnDocument, lineChargesElement,
						LabsConstants.E_LINE_CHARGE, null);
				lineChargeElement.setAttribute(LabsConstants.A_CHARGE_CATEGORY,
						element.getAttribute(LabsConstants.A_CHARGE_CATEGORY));
				lineChargeElement.setAttribute(LabsConstants.A_CHARGE_NAME,
						element.getAttribute(LabsConstants.A_CHARGE_NAME));
				lineChargeElement.setAttribute(LabsConstants.A_CHARGE_PER_UNIT,
						element.getAttribute(LabsConstants.A_CHARGE_PER_UNIT));
				lineChargeElement.setAttribute(LabsConstants.A_CHARGE_PER_LINE,
						element.getAttribute(LabsConstants.A_CHARGE_PER_LINE));
			}
		}
		logger.endTimer(sMethodName);
	}

	private String orderFOCIHold(YFSEnvironment env, Document inputDocument)
			throws XPathException {

		String strMethodName = "CreateEventPayload.orderAmended()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_FOCI_HOLD;

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_FOCI_HOLD);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFOCIHoldEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	private String orderAmended(YFSEnvironment env, Document inputDocument)
			throws XPathException {

		String strMethodName = "CreateEventPayload.orderAmended()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_AMENDED;

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_AMENDED);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderAmendedEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws JSONException
	 * @throws XPathException
	 * @throws TransformerException
	 * @throws ParserConfigurationException
	 */
	private String poLine(Document inputDocument) throws JSONException,
			XPathException, ParserConfigurationException, TransformerException {

		String strMethodName = "CreateEventPayload.poLine()";
		logger.beginTimer("Starting " + strMethodName);

		Element elePOLine = inputDocument.getDocumentElement();
		Element eleOrder = XMLUtils.getElementFromXPath(inputDocument,
				"OrderStatusChange/OrderAudit/Order");

		String sPONo = elePOLine.getAttribute(EventBusConstants.ORDER_NO);
		String sOrderNo = eleOrder
				.getAttribute(EventBusConstants.CUSTOMER_PO_NO);
		String sShipNode = eleOrder.getAttribute(EventBusConstants.SHIP_NODE);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_PO_ACK);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_PO_ACK);

		Preconditions.checkNotNull(sOrderNo, "OrderNo can't be null");
		Preconditions.checkNotNull(sPONo, "CustomerPONo can't be null");
		Preconditions.checkNotNull(sShipNode, "ShipNode can't be null");

		com.walmart.services.order.common.model.Order orderDTO = new com.walmart.services.order.common.model.Order();
		PurchaseOrder purchaseOrderDTO = new PurchaseOrder();
		orderDTO.setOrderNo(sOrderNo);
		purchaseOrderDTO.setPurchaseOrderNo(sPONo);
		purchaseOrderDTO.setShipNode(sShipNode);

		List<PurchaseOrder> lPOList = new ArrayList<PurchaseOrder>();
		lPOList.add(purchaseOrderDTO);
		orderDTO.setPurchaseOrders(lPOList);

		NodeList orderAudits = elePOLine
				.getElementsByTagName(EventBusConstants.ORDER_AUDIT);
		if (orderAudits.getLength() > 0) {
			Element orderAudit = (Element) orderAudits.item(0);
			String dModifyts = null;
			if (orderAudit != null) {
				dModifyts = orderAudit.getAttribute(EventBusConstants.MODIFYTS);
			} else {
				dModifyts = DateUtils.getCurrentDateISOWithTz();
			}
			if (!YFCCommon.isVoid(dModifyts)) {
				orderDTO.setLastModified(new Timestamp(DateUtils
						.getMilliSecs(dModifyts)));
			}
		}

		JSONObject jsonStr = new JSONObject(orderDTO.toOrderJsonString());

		logger.endTimer("Exiting Execution of" + strMethodName);
		return jsonStr.toString();
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws JSONException
	 * @throws XPathException
	 * @throws TransformerException
	 * @throws ParserConfigurationException
	 */
	private String poPickInProgress(Document inputDocument)
			throws JSONException, XPathException, ParserConfigurationException,
			TransformerException {

		String strMethodName = "CreateEventPayload.poPickInProgress()";
		logger.beginTimer("Starting " + strMethodName);

		Element elePOPickInProgress = inputDocument.getDocumentElement();
		Element eleOrder = XMLUtils.getElementFromXPath(inputDocument,
				"OrderStatusChange/OrderAudit/Order");

		String sPONo = elePOPickInProgress
				.getAttribute(EventBusConstants.ORDER_NO);
		String sOrderNo = eleOrder
				.getAttribute(EventBusConstants.CUSTOMER_PO_NO);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_PO_ACK);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_PO_ACK);

		Preconditions.checkNotNull(sOrderNo, "OrderNo can't be null");
		Preconditions.checkNotNull(sPONo, "CustomerPONo can't be null");

		com.walmart.services.order.common.model.Order orderDTO = new com.walmart.services.order.common.model.Order();
		PurchaseOrder purchaseOrderDTO = new PurchaseOrder();
		orderDTO.setOrderNo(sOrderNo);

		List<Element> elePOOrderLines = XMLUtils
				.getElementListByXpath(inputDocument,
						"OrderStatusChange/OrderAudit/OrderAuditLevels/OrderAuditLevel/OrderLine");
		List<PurchaseOrderLine> lPOOLineList = new ArrayList<PurchaseOrderLine>();

		for (Element elePOOrderLine : elePOOrderLines) {

			PurchaseOrderLine elePOOLine = new PurchaseOrderLine();

			String strPrimeLineNo = elePOOrderLine
					.getAttribute(LabsConstants.A_PRIME_LINE_NO);
			elePOOLine.setPrimeLineNo(Integer.parseInt(strPrimeLineNo));

			Element eleOLItemAudit = XMLUtils
					.getElementFromXPath(
							inputDocument,
							"//OrderStatusChange/OrderAudit/OrderAuditLevels/OrderAuditLevel/OrderLine[@PrimeLineNo='"
									+ strPrimeLineNo + "']/Item");

			Measurement mPOLine = new Measurement();
			mPOLine.setMeasurementValue(new BigDecimal(elePOOrderLine
					.getAttribute(LabsConstants.A_ORDERED_QUANTITY)));

			UnitOfMeasureEnum unitOfMeasure = null;
			mPOLine.setUnitOfMeasure(unitOfMeasure.fromCode(eleOLItemAudit
					.getAttribute("UnitOfMeasure")));

			elePOOLine.setOrderedQty(mPOLine);

			List<PoLineStatusInfo> poLineStatusInfoList = new ArrayList<PoLineStatusInfo>();
			PoLineStatusInfo poLineStatusInfo = new PoLineStatusInfo();

			if ((EventBusConstants.EventEnum.PO_PICK_IN_PROGRESS).toString()
					.equalsIgnoreCase(EVENT)) {
				poLineStatusInfo.setPoLineStatus("PO Pick In Progress");
				poLineStatusInfo.setPoLineStatusCode("1100.450");
				poLineStatusInfo
						.setPoLineStatusDescription("PO Pick In Progress");
			} else if ((EventBusConstants.EventEnum.PO_UNRECOGNIZED).toString()
					.equalsIgnoreCase(EVENT)) {
				poLineStatusInfo.setPoLineStatus("PO Unrecognized");
				poLineStatusInfo.setPoLineStatusCode("1100.800");
				poLineStatusInfo.setPoLineStatusDescription("PO Unrecognized");
			}
			poLineStatusInfo.setPoLineStatusChangeDate(DateUtils
					.getCurrentDate());
			poLineStatusInfo.setPoLineStatusQuantity(mPOLine);

			poLineStatusInfoList.add(0, poLineStatusInfo);
			elePOOLine.setPoLineStatusInfos(poLineStatusInfoList);
			lPOOLineList.add(elePOOLine);
		}

		purchaseOrderDTO.setPurchaseOrderLines(lPOOLineList);
		purchaseOrderDTO.setPurchaseOrderNo(sPONo);

		List<PurchaseOrder> lPOList = new ArrayList<PurchaseOrder>();
		lPOList.add(purchaseOrderDTO);
		orderDTO.setPurchaseOrders(lPOList);

		NodeList orderAudits = elePOPickInProgress
				.getElementsByTagName(EventBusConstants.ORDER_AUDIT);
		if (orderAudits.getLength() > 0) {
			Element orderAudit = (Element) orderAudits.item(0);
			String dModifyts = null;
			if (orderAudit != null) {
				dModifyts = orderAudit.getAttribute(EventBusConstants.MODIFYTS);
			} else {
				dModifyts = DateUtils.getCurrentDateISOWithTz();
			}
			if (!YFCCommon.isVoid(dModifyts)) {
				orderDTO.setLastModified(new Timestamp(DateUtils
						.getMilliSecs(dModifyts)));
			}
		}

		JSONObject jsonStr = new JSONObject(orderDTO.toOrderJsonString());

		logger.endTimer("Exiting Execution of" + strMethodName);
		return jsonStr.toString();
	}

	private String poPickComplete(Document inputDocument)
			throws ParserConfigurationException, JSONException, XPathException,
			TransformerException {
		String strMethodName = "CreateEventPayload.orderFulfillmentCreate()";
		logger.beginTimer("Starting " + strMethodName);

		Element elePOPick = inputDocument.getDocumentElement();

		Element eleSOOrder = XMLUtils.getElementFromXPath(inputDocument,
				"OrderStatusChange/OrderAudit/Order");
		String sPONo = elePOPick.getAttribute(EventBusConstants.ORDER_NO);
		String sOrderNo = eleSOOrder
				.getAttribute(EventBusConstants.CUSTOMER_PO_NO);
		String dModifyts = ((Element) elePOPick.getElementsByTagName(
				EventBusConstants.ORDER_AUDIT).item(0))
				.getAttribute(EventBusConstants.MODIFYTS);
		String shipNode = eleSOOrder.getAttribute(EventBusConstants.SHIP_NODE);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_PO_ACK);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_PO_ACK);

		Preconditions.checkNotNull(sOrderNo, "OrderNo can't be null");
		Preconditions.checkNotNull(sPONo, "CustomerPONo can't be null");

		com.walmart.services.order.common.model.Order orderDTO = new com.walmart.services.order.common.model.Order();
		PurchaseOrder purchaseOrderDTO = new PurchaseOrder();
		orderDTO.setOrderNo(sOrderNo);

		List<Element> elePOOrderLines = XMLUtils.getElementListByXpath(
				inputDocument, "OrderStatusChange/OrderLines/OrderLine");
		List<PurchaseOrderLine> lPOOLineList = new ArrayList<PurchaseOrderLine>();

		for (Element elePOOrderLine : elePOOrderLines) {
			PurchaseOrderLine elePOOLine = new PurchaseOrderLine();

			String strPrimeLineNo = elePOOrderLine
					.getAttribute(LabsConstants.A_PRIME_LINE_NO);
			elePOOLine.setPrimeLineNo(Integer.parseInt(strPrimeLineNo));

			Element eleOLItemAudit = XMLUtils
					.getElementFromXPath(
							inputDocument,
							"//OrderStatusChange/OrderAudit/OrderAuditLevels/OrderAuditLevel/OrderLine[@PrimeLineNo='"
									+ strPrimeLineNo + "']/Item");

			Measurement mPOLine = new Measurement();
			mPOLine.setMeasurementValue(new BigDecimal(elePOOrderLine
					.getAttribute(LabsConstants.A_QUANTITY)));

			UnitOfMeasureEnum unitOfMeasure = null;
			mPOLine.setUnitOfMeasure(unitOfMeasure.fromCode(eleOLItemAudit
					.getAttribute("UnitOfMeasure")));

			elePOOLine.setOrderedQty(mPOLine);
			lPOOLineList.add(elePOOLine);
		}
		if (!YFCCommon.isVoid(dModifyts))
			orderDTO.setLastModified(new Timestamp(DateUtils
					.getMilliSecs(dModifyts)));

		purchaseOrderDTO.setPurchaseOrderNo(sPONo);
		purchaseOrderDTO.setShipNode(shipNode);
		purchaseOrderDTO.setPurchaseOrderLines(lPOOLineList);
		List<PurchaseOrder> lPOList = new ArrayList<PurchaseOrder>();

		lPOList.add(purchaseOrderDTO);

		orderDTO.setPurchaseOrders(lPOList);

		JSONObject jsonStr = new JSONObject(orderDTO.toOrderJsonString());

		logger.endTimer("Exiting Execution of" + strMethodName);
		return jsonStr.toString();
	}

	/**
	 * @param inputDocument
	 * @return
	 * @throws JSONException
	 * @throws XPathExpressionException
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 * @throws TransformerException
	 */
	private String poArrivedAtStore(YFSEnvironment env, Document inputDocument)
			throws JSONException, XPathException, ParserConfigurationException,
			SAXException, IOException, TransformerException {

		String strMethodName = "CreateEventPayload.poArrivedAtStore()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_SHIPMENT_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_SHIPMENT_ORDER);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_FULFILLMENTORDER_SHIPPED);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ strMethodName);

		String jsonPOShipmentStr = LabsDozerMapper
				.generateMappedJson(
						Shipment.class,
						com.walmart.services.order.common.model.PurchaseOrderShipment.class,
						inputDocument, dbMapper);

		DozerBeanMapper dbMapper1 = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ARRIVED_AT_STORE);

		if (dbMapper1 == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ strMethodName);

		String jsonPOShipmentStr1 = LabsDozerMapper.generateMappedJson(
				Shipment.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper1);

		JSONArray po = new JSONArray();
		po.put(0, new JSONObject(jsonPOShipmentStr));

		JSONObject jsonStr = new JSONObject(jsonPOShipmentStr1);
		JSONArray purchaseOrders = jsonStr.getJSONArray("purchaseOrders");
		JSONObject purchaseOrder = (JSONObject) purchaseOrders.get(0);
		purchaseOrder.put("purchaseOrderShipments", po);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr.toString();
	}

	/**
	 * @param inputDocument
	 * @return
	 * @throws JSONException
	 * @throws XPathExpressionException
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 * @throws TransformerException
	 */
	private String poShipped(YFSEnvironment env, Document inputDocument)
			throws JSONException, XPathException, ParserConfigurationException,
			SAXException, IOException, TransformerException {

		String strMethodName = "CreateEventPayload.poShipped()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_SHIPMENT_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_SHIPMENT_ORDER);

		if (VERTICALID.equalsIgnoreCase("23")) {
			fetchAndUpdateRefLineID(env, inputDocument);
		}

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_FULFILLMENTORDER_SHIPPED);

		JSONObject json = new JSONObject();

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ strMethodName);

		Element shipmentEle = inputDocument.getDocumentElement();

		List<Element> shipmentLines = XMLUtils.getElementListByTagName(
				shipmentEle, "ShipmentLine");

		JSONArray poNumber = new JSONArray();
		JSONObject poJson = new JSONObject();

		NodeList itemList = shipmentEle.getElementsByTagName("Item");
		JSONArray poLines = new JSONArray();

		json.put("orderNo", shipmentLines.get(0).getAttribute("CustomerPoNo"));

		if (!YFCCommon.isVoid(XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_CUSTCUSTPONO_SHIPMENT_ORDER))) {
			json.put("originSystemOrderId", XMLUtils.getAttributeFromXPath(
					inputDocument,
					EventBusConstants.XPATH_CUSTCUSTPONO_SHIPMENT_ORDER));
		}

		if ((EventBusConstants.EventEnum.PO_SHIPPED).toString()
				.equalsIgnoreCase(EVENT)
				|| (EventBusConstants.EventEnum.SHIPMENT_INVOICED).toString()
						.equalsIgnoreCase(EVENT)
				|| (EventBusConstants.EventEnum.PO_SHIPPED_DELIVERY_DATE_UPDATE)
						.toString().equalsIgnoreCase(EVENT)
				|| (EventBusConstants.EventEnum.PO_SHIPMENT_TD_EDD_UPDATE)
						.toString().equalsIgnoreCase(EVENT)) {

			json.put("orderSource", XMLUtils.getAttributeFromXPath(
					inputDocument,
					EventBusConstants.XPATH_ENTRYTYPE_SHIPMENT_ORDER));
			json.put("orderOrigin", XMLUtils.getAttributeFromXPath(
					inputDocument,
					EventBusConstants.XPATH_EXTNORDERORIGIN_SHIPMENT_ORDER));
			poJson.put("shipNodeType", XMLUtils.getAttributeFromXPath(
					inputDocument,
					EventBusConstants.XPATH_SHIPNODE_TYPE_SHIPMENT_ORDER));
			JSONObject customAttributes = new JSONObject();
			customAttributes
					.put("sellerOrderNo",
							XMLUtils.getAttributeFromXPath(
									inputDocument,
									EventBusConstants.XPATH_SELLERORDERNO_SHIPMENT_ORDER));
			customAttributes.put("martId", XMLUtils.getAttributeFromXPath(
					inputDocument,
					EventBusConstants.XPATH_MARTID_SHIPMENT_ORDER));
			customAttributes.put("businessUnitId", XMLUtils
					.getAttributeFromXPath(inputDocument,
							EventBusConstants.XPATH_BUID_SHIPMENT_ORDER));
			json.put("orderCustomAttributes", customAttributes);

		}

		Document getOrderLineList = null;
		boolean poDatePresent = false;
		for (int i = 0; i < shipmentLines.size(); i++) {

			Element shipmentLine = shipmentLines.get(i);
			poJson.put("purchaseOrderNo", shipmentLine.getAttribute("OrderNo"));

			NodeList orderLines = shipmentLine
					.getElementsByTagName("OrderLine");

			int numberOfOrderLines = orderLines.getLength();
			for (int j = 0; j < numberOfOrderLines; j++) {

				Element orderLine = (Element) orderLines.item(j);
				String OLKey = orderLine
						.getAttribute("ChainedFromOrderLineKey");

				JSONObject poLine = new JSONObject();
				poLine.put("poLineId", orderLine.getAttribute("PrimeLineNo"));

				// Adding reference Line Id
				Element eleChainedOL = (Element) orderLine
						.getElementsByTagName("ChainedFromOrderLine").item(0);
				if (eleChainedOL != null) {
					Element eleCustomAttributes = (Element) eleChainedOL
							.getElementsByTagName("CustomAttributes").item(0);
					if (eleCustomAttributes != null
							&& !YFCCommon.isVoid(eleCustomAttributes
									.getAttribute("ReferenceLineId"))) {
						String strRefLineId = eleCustomAttributes
								.getAttribute("ReferenceLineId");
						poLine.put("referenceLineId", strRefLineId);
					} else if (VERTICALID.equalsIgnoreCase("23")) {
						if (getOrderLineList == null) {
							Document getOrderLineListInDoc = XMLUtils
									.createDocument("OrderLine");
							getOrderLineListInDoc
									.getDocumentElement()
									.setAttribute(
											"OrderHeaderKey",
											orderLine
													.getAttribute("ChainedFromOrderHeaderKey"));
							Document template = XMLUtils
									.getDocument("<OrderLineList><OrderLine OrderLineKey=''><CustomAttributes ReferenceLineId='' Text8=''/></OrderLine></OrderLineList>");
							getOrderLineList = LabsCommonUtil.invokeAPI(env,
									template, "getOrderLineList",
									getOrderLineListInDoc);
						}
						Element matchOrderLine = XMLUtils.getElementFromXPath(
								getOrderLineList,
								"OrderLineList/OrderLine[@OrderLineKey = '"
										+ OLKey + "']");
						Element customAttributes = null;
						String referenceLineId = null;
						String text8 = null;
						if (matchOrderLine != null)
							customAttributes = (Element) matchOrderLine
									.getElementsByTagName("CustomAttributes")
									.item(0);
						if (customAttributes != null)
							referenceLineId = customAttributes
									.getAttribute("ReferenceLineId");
						text8 = customAttributes.getAttribute("Text8");
						if (!YFCCommon.isVoid(referenceLineId)
								|| !YFCCommon.isVoid(referenceLineId)) {
							if (!YFCCommon.isVoid(referenceLineId)) {
								poLine.put("referenceLineId", referenceLineId);
							} else {
								poLine.put("referenceLineId", text8);
							}
						} else {
							throw LabsHandler.handleException("ERR-REFLINEID",
									"Reference Line Id Missing ");
						}
					}
				}

				JSONArray poDates = new JSONArray();

				Element orderDates = (Element) orderLine.getElementsByTagName(
						"OrderDates").item(0);
				if (null != orderDates && !poDatePresent) {
					List<Element> orderDateList = XMLUtils
							.getElementListByTagName(orderDates, "OrderDate");
					for (int k = 0; k < orderDateList.size(); k++) {
						JSONObject poDate = new JSONObject();
						Element orderDate = orderDateList.get(k);
						poDate.put("dateTypeId",
								orderDate.getAttribute("DateTypeId"));
						poDate.put("actualDate",
								orderDate.getAttribute("ActualDate"));
						poDate.put("expectedDate",
								orderDate.getAttribute("ExpectedDate"));
						poDates.put(k, poDate);

						// Start - Adding udpated ship date received from MCSE

						if ((EventBusConstants.EventEnum.PO_SHIPMENT_TD_EDD_UPDATE
								.toString().equalsIgnoreCase(EVENT))
								|| (EventBusConstants.EventEnum.PO_SHIPPED_DELIVERY_DATE_UPDATE)
										.toString().equalsIgnoreCase(EVENT)) {
							String newShipDate = orderDate
									.getAttribute("Extn_NewExpectedDate");
							JSONObject poCustomAttributes = (JSONObject) poJson
									.opt("poCustomAttributes");

							if (YFCCommon.isVoid(poCustomAttributes)) {
								poCustomAttributes = new JSONObject();
								poJson.put("poCustomAttributes",
										poCustomAttributes);
							}
							if (!StringUtils.isNullOrEmpty(newShipDate)) {
								poCustomAttributes.put("updatedActualShipDate",
										newShipDate);
							}

							Element eleAudit = XMLUtils
									.getElementFromXPath(shipmentEle,
											"/Shipment/ShipmentStatusAudits/ShipmentStatusAudit[@NewStatus = '1600.10']");
							if (!YFCCommon.isVoid(eleAudit)) {
								poCustomAttributes.put("trailerDepartDate",
										eleAudit.getAttribute("NewStatusDate"));
							}
						}
						// End - Adding udpated ship date received from MCSE

					}
					poJson.put("poDate", poDates);
					poDatePresent = true;
				}

				JSONObject orderedQty = new JSONObject();
				orderedQty.put("measurementValue",
						orderLine.getAttribute("OrderedQty"));
				orderedQty.put("unitOfMeasure", ((Element) itemList.item(j))
						.getAttribute("UnitOfMeasure"));

				poLine.put("orderedQty", orderedQty);

				JSONArray poLineStatusInfos = new JSONArray();
				JSONObject poLineStatusInfo = new JSONObject();

				if ((EventBusConstants.EventEnum.PO_SHIPPED).toString()
						.equalsIgnoreCase(EVENT)
						|| (EventBusConstants.EventEnum.PO_SHIPPED_DELIVERY_DATE_UPDATE)
								.toString().equalsIgnoreCase(EVENT)
						|| (EventBusConstants.EventEnum.PO_SHIPMENT_TD_EDD_UPDATE)
								.toString().equalsIgnoreCase(EVENT)) {
					if ("PUT".equalsIgnoreCase(((Element) orderLines.item(0))
							.getAttribute("FulfillmentType"))) {
						if (!(EventBusConstants.EventEnum.PO_SHIPPED_DELIVERY_DATE_UPDATE)
								.toString().equalsIgnoreCase(EVENT)
								&& !(EventBusConstants.EventEnum.PO_SHIPMENT_TD_EDD_UPDATE)
										.toString().equalsIgnoreCase(EVENT))
							EVENT = "PO_PICKING_COMPLETE";
						poLineStatusInfo.put("poLineStatus",
								"PO Picking Complete");
						poLineStatusInfo.put("poLineStatusCode", "3700.0001");
						poLineStatusInfo.put("poLineStatusDescription",
								"PO Picking Complete");
					} else {
						poLineStatusInfo.put("poLineStatus", "PO Shipped");
						poLineStatusInfo.put("poLineStatusCode", "3700");
						poLineStatusInfo.put("poLineStatusDescription",
								"PO Shipped");

						NodeList references = XMLUtils.getNodeList(orderLine,
								"References/Reference");
						int numberOfReferences = references.getLength();

						if (numberOfReferences > 0) {
							JSONArray extrinsicList = new JSONArray();
							for (int k = 0; k < numberOfReferences; k++) {

								Element reference = (Element) references
										.item(k);
								JSONObject extrinsic = new JSONObject();

								extrinsic.put("name",
										reference.getAttribute("Name"));
								extrinsic.put("value",
										reference.getAttribute("Value"));
								extrinsicList.put(k, extrinsic);
							}
							poLine.put("extrinsicList", extrinsicList);
						}
					}
				} else if ((EventBusConstants.EventEnum.SHIPMENT_INVOICED)
						.toString().equalsIgnoreCase(EVENT)) {
					poLineStatusInfo.put("poLineStatus",
							"PO Shipped AND Invoiced");
					poLineStatusInfo.put("poLineStatusCode", "3700");
					poLineStatusInfo.put("poLineStatusDescription",
							"PO Shipped and Invoiced");

					NodeList references = XMLUtils.getNodeList(orderLine,
							"References/Reference");
					int numberOfReferences = references.getLength();

					if (numberOfReferences > 0) {
						JSONArray extrinsicList = new JSONArray();
						for (int k = 0; k < numberOfReferences; k++) {

							Element reference = (Element) references.item(k);
							JSONObject extrinsic = new JSONObject();

							extrinsic.put("name",
									reference.getAttribute("Name"));
							extrinsic.put("value",
									reference.getAttribute("Value"));
							extrinsicList.put(k, extrinsic);
						}
						poLine.put("extrinsicList", extrinsicList);
					}
				} else if ((EventBusConstants.EventEnum.PO_DELIVERED)
						.toString().equalsIgnoreCase(EVENT)) {
					poLineStatusInfo.put("poLineStatus", "PO Delivered");
					poLineStatusInfo.put("poLineStatusCode", "3700.4031");
					poLineStatusInfo.put("poLineStatusDescription",
							"PO Delivered");
				} else if ((EventBusConstants.EventEnum.PO_BINNING_COMPLETE)
						.toString().equalsIgnoreCase(EVENT)) {
					poLineStatusInfo.put("poLineStatus", "PO Binning Complete");
					poLineStatusInfo.put("poLineStatusCode", "1600.4222");
					poLineStatusInfo.put("poLineStatusDescription",
							"PO Binning Complete");
				}
				poLineStatusInfo.put("poLineStatusChangeDate", DateUtils
						.getMilliSecs(DateUtils
								.getCurrentDateInSterlingDateFormat()));

				JSONObject poLineStatusQuantity = new JSONObject();
				poLineStatusQuantity.put("unitOfMeasure",
						shipmentLine.getAttribute("UnitOfMeasure"));
				poLineStatusQuantity.put("measurementValue",
						shipmentLine.getAttribute("Quantity"));

				poLineStatusInfo.put("poLineStatusQuantity",
						poLineStatusQuantity);
				poLineStatusInfos.put(0, poLineStatusInfo);

				poLine.put("poLineStatusInfos", poLineStatusInfos);
				poLines.put(poLine);
			}
		}

		String jsonPOShipmentStr = LabsDozerMapper
				.generateMappedJson(
						Shipment.class,
						com.walmart.services.order.common.model.PurchaseOrderShipment.class,
						inputDocument, dbMapper);

		JSONArray po = new JSONArray();
		po.put(0, new JSONObject(jsonPOShipmentStr));

		poJson.put("purchaseOrderLines", poLines);
		poJson.put("purchaseOrderShipments", po);
		poNumber.put(0, poJson);
		json.put("purchaseOrders", poNumber);

		logger.debug("JSON" + json);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return json.toString();
	}

	/**
	 * @param inputDocument
	 * @return
	 * @throws JSONException
	 * @throws XPathExpressionException
	 * @throws ParserConfigurationException
	 */
	private String associateDelivery(Document inputDocument)
			throws JSONException, XPathException, ParserConfigurationException {

		String strMethodName = "CreateEventPayload.associateDelivery()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_SHIPMENT_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_SHIPMENT_ORDER);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_FULFILLMENTORDER_AD);

		com.walmart.services.order.common.model.Order orderObject = new com.walmart.services.order.common.model.Order();

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ strMethodName);

		Element shipmentEle = inputDocument.getDocumentElement();

		List<Element> shipmentLines = XMLUtils.getElementListByTagName(
				shipmentEle, "ShipmentLine");
		PurchaseOrder po = new PurchaseOrder();

		orderObject.setOrderNo(shipmentLines.get(0)
				.getAttribute("CustomerPoNo"));
		orderObject.setOriginSystemOrderId(XMLUtils.getAttributeFromXPath(
				inputDocument,
				EventBusConstants.XPATH_CUSTCUSTPONO_SHIPMENT_ORDER));
		orderObject.setOrderSource(XMLUtils
				.getAttributeFromXPath(inputDocument,
						EventBusConstants.XPATH_ENTRYTYPE_SHIPMENT_ORDER));
		orderObject.setOrderOrigin(XMLUtils.getAttributeFromXPath(
				inputDocument,
				EventBusConstants.XPATH_EXTNORDERORIGIN_SHIPMENT_ORDER));

		Element shipmentLine = XMLUtils.getFirstElementByName(shipmentEle,
				"ShipmentLines/ShipmentLine");
		Element orderLine = XMLUtils.getFirstElementByName(shipmentLine,
				LabsConstants.E_ORDER_LINE);
		po.setPurchaseOrderNo(shipmentLine.getAttribute("OrderNo"));

		final List<PurchaseOrderShipment> shipments = new ArrayList<PurchaseOrderShipment>();

		PurchaseOrderShipment jsonPOShipmentStr = (PurchaseOrderShipment) LabsDozerMapper
				.generateMappedObject(
						Shipment.class,
						com.walmart.services.order.common.model.PurchaseOrderShipment.class,
						inputDocument, dbMapper);
		Element eleToAddressFromShipment = XMLUtils.getFirstElementByName(
				shipmentEle, "ToAddress");
		logger.debug("The documents is ",
				XMLUtils.getDocumentForElement(eleToAddressFromShipment));
		ContactInfo shipToAddress = (ContactInfo) LabsDozerMapper
				.generateMappedObject(
						com.walmartlabs.services.util.jaxb.shipmentlist.ToAddress.class,
						ContactInfo.class,
						XMLUtils.getDocumentForElement(eleToAddressFromShipment),
						dbMapper);
		shipments.add(jsonPOShipmentStr);
		po.setPurchaseOrderShipments(shipments);
		po.setShipToAddress(shipToAddress);
		final List<PurchaseOrder> purchaseOrders = new ArrayList<PurchaseOrder>();
		purchaseOrders.add(po);
		orderObject.setPurchaseOrders(purchaseOrders);
		logger.debug("The orderObject is ", orderObject.toString());
		return orderObject.toString();
	}

	/**
	 * 
	 * @param inputDocument
	 * @return
	 * @throws ParserConfigurationException
	 * @throws JSONException
	 * @throws XPathException
	 */
	private String orderHoldChange(Document inputDocument)
			throws ParserConfigurationException, JSONException, XPathException {

		String strMethodName = "CreateEventPayload.orderHoldChange()";
		logger.beginTimer("Starting " + strMethodName);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_HOLDCHANGE_ORDER);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for orderHoldChangeEvent" + strMethodName);

		JSONObject json = null;

		String jsonHoldStr = LabsDozerMapper.generateMappedJson(
				OrderHoldType.class,
				com.walmart.services.order.common.model.OrderHoldType.class,
				inputDocument, dbMapper);

		NodeList nlOrder = inputDocument
				.getElementsByTagName(EventBusConstants.ORDER);
		Element eleOrder;
		if (nlOrder.getLength() > 0) {
			eleOrder = (Element) nlOrder.item(0);
			VERTICALID = XMLUtils.getAttributeFromXPath(eleOrder,
					EventBusConstants.XPATH_VERTICALID_ORDER_HOLD);
			TENANTID = XMLUtils.getAttributeFromXPath(eleOrder,
					EventBusConstants.XPATH_TENANTID_ORDER_HOLD);
			SCHEMAID = EventBusConstants.SCHEMA_ORDER_HDRHOLDCHANGE;
			String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
					com.walmart.services.order.common.model.Order.class,
					XMLUtils.getDocumentForElement(eleOrder), dbMapper);
			json = new JSONObject(jsonStr);
		} else {
			logger.debug("Order details missing");
			throw LabsHandler.handleException(
					LabsConstants.ERROR_MANDATORY_PARAMS_MISSING,
					"Order details are missing in the template for Order Hold event"
							+ strMethodName);
		}

		JSONArray orderHolds = new JSONArray();
		orderHolds.put(0, new JSONObject(jsonHoldStr));
		json.put("holds", orderHolds);
		logger.debug("JSON" + json);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return json.toString();
	}

	/**
	 * 
	 * @param inputDocument
	 * @return
	 * @throws ParserConfigurationException
	 * @throws JSONException
	 * @throws XPathException
	 */
	private String orderLineHoldChange(Document inputDocument)
			throws ParserConfigurationException, JSONException, XPathException {

		String strMethodName = "CreateEventPayload.orderLineHoldChange()";
		logger.beginTimer("Starting " + strMethodName);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_HOLDCHANGE_ORDERLINE);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for orderLineHoldChange Event"
							+ strMethodName);

		JSONObject json = null;
		NodeList nlOrder = inputDocument
				.getElementsByTagName(EventBusConstants.ORDER);
		Element eleOrder;
		if (nlOrder.getLength() > 0) {
			eleOrder = (Element) nlOrder.item(0);
			VERTICALID = XMLUtils.getAttributeFromXPath(eleOrder,
					EventBusConstants.XPATH_VERTICALID_ORDER_HOLD);
			TENANTID = XMLUtils.getAttributeFromXPath(eleOrder,
					EventBusConstants.XPATH_TENANTID_ORDER_HOLD);
			SCHEMAID = EventBusConstants.SCHEMA_ORDER_LINEHOLDCHANGE;
			String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
					com.walmart.services.order.common.model.Order.class,
					XMLUtils.getDocumentForElement(eleOrder), dbMapper);
			json = new JSONObject(jsonStr);
			NodeList nlOrderLine = inputDocument
					.getElementsByTagName(EventBusConstants.ORDER_LINE);
			int lineLen = nlOrderLine.getLength();
			JSONArray orderLines = new JSONArray();
			if (lineLen > 0) {
				for (int i = 0; i < lineLen; i++) {
					Element orderLine = (Element) nlOrderLine.item(i);
					String jsonLineStr = LabsDozerMapper
							.generateMappedJson(
									OrderLine.class,
									com.walmart.services.order.common.model.OrderLine.class,
									XMLUtils.getDocumentForElement(orderLine),
									dbMapper);
					JSONObject jsonLine = new JSONObject(jsonLineStr);

					NodeList nlHolds = orderLine
							.getElementsByTagName(EventBusConstants.ORDER_HOLD_TYPE);
					int holdLen = nlHolds.getLength();
					JSONArray orderLineHolds = new JSONArray();

					for (int j = 0; j < holdLen; j++) {
						Element orderHoldType = (Element) nlHolds.item(j);
						String jsonHoldStr1 = LabsDozerMapper
								.generateMappedJson(
										OrderHoldType.class,
										com.walmart.services.order.common.model.OrderHoldType.class,
										XMLUtils.getDocumentForElement(orderHoldType),
										dbMapper);
						orderLineHolds.put(j, new JSONObject(jsonHoldStr1));
					}
					if (orderLineHolds.length() > 0)
						jsonLine.put("holds", orderLineHolds);
					else {
						logger.debug("Line hold details missing");
						throw LabsHandler.handleException(
								LabsConstants.ERROR_MANDATORY_PARAMS_MISSING,
								"Line hold details are missing in the template for OrderLine Hold event"
										+ strMethodName);
					}
					orderLines.put(i, jsonLine);
				}
				json.put("orderLines", orderLines);
			} else {
				logger.debug("OrderLine details missing");
				throw LabsHandler.handleException(
						LabsConstants.ERROR_MANDATORY_PARAMS_MISSING,
						"OrderLine details are missing in the template for OrderLine Hold event"
								+ strMethodName);
			}
		} else {
			logger.debug("Order details missing");
			throw LabsHandler.handleException(
					LabsConstants.ERROR_MANDATORY_PARAMS_MISSING,
					"Order details are missing in the template for OrderLine Hold event"
							+ strMethodName);
		}

		logger.debug("JSON" + json);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return json.toString();
	}

	/**
	 * 
	 * @param sEventMsg
	 * @return
	 */
	private String appendHeaders(String sEventMsg) {
		String strMethodName = "CreateEventPayload.appendHeaders()";
		logger.beginTimer(strMethodName);

		JSONObject sJson = new JSONObject();
		JSONObject header = new JSONObject();
		try {
			header.put(EventBusConstants.SCHEMAID, SCHEMAID);
			header.put(EventBusConstants.VERTICAL, VERTICALID);
			header.put(EventBusConstants.TENANT, TENANTID);
			header.put(EventBusConstants.EVENTNAME, EVENT);
			header.put(EventBusConstants.SOURCE, EventBusConstants.SOURCE_PGOMS);
			header.put(EventBusConstants.EVENTID, eventID);
			header.put(EventBusConstants.EVENTDTS, DateUtils
					.getMilliSecs(DateUtils
							.getCurrentDateInSterlingDateFormat()));
			sJson.put(EventBusConstants.HEADER, header);
			sJson.put(EventBusConstants.PAYLOAD, new JSONObject(sEventMsg));
		} catch (JSONException je) {
			throw LabsHandler.handleException(
					LabsConstants.ERROR_JSON_EXCEPTION,
					"JSONException in CreateEventPayload.appendHeaders", je);
		}

		logger.endTimer("Exiting Execution of" + strMethodName);
		return sJson.toString();
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 */
	private String orderNotes(Document inputDocument) throws XPathException {

		String strMethodName = "CreateEventPayload.orderNotes()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_NOTE;

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_NOTES);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderNotesEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 * @throws TransformerException
	 * @throws ParserConfigurationException
	 */
	private String orderCancel(Document inputDocument) throws XPathException,
			ParserConfigurationException, TransformerException {

		String strMethodName = "CreateEventPayload.orderCancel()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_CANCEL;

		// check if order is fully cancelled, then pass reference indicator in
		// the run time
		String sMinOrderStatus = inputDocument.getDocumentElement()
				.getAttribute("MinOrderStatus");
		String sDraftOrder = inputDocument.getDocumentElement().getAttribute(
				"DraftOrderFlag");
		String sAuditTranId = inputDocument.getDocumentElement().getAttribute(
				"AuditTransactionId");

		if ("FILL_OR_KILL_CANCEL".equals(sAuditTranId)) {
			inputDocument = LabsEventUtils
					.filterRepeatedOfferIdLines(inputDocument);
		}

		if ((!sMinOrderStatus.equals(null) && sMinOrderStatus.contains("9000"))
				|| (("Y").equals(sDraftOrder) && ("I2C_ORDER_CANCEL")
						.equals(sAuditTranId))) {
			Element eleReferences = null;
			eleReferences = XMLUtils.getFirstElementByName(
					inputDocument.getDocumentElement(), "References");
			if (null == eleReferences) {
				eleReferences = XMLUtils.createElement(inputDocument,
						"References", null);
			}

			// this means order is fully cancelled
			Element eleReference = XMLUtils.createElement(inputDocument,
					"Reference", null);
			eleReference.setAttribute("Name", "isCompleteOrderCancel");
			eleReference.setAttribute("Value", "YES");
			eleReferences.appendChild(eleReference);

		}

		/*
		 * go through the order audit if available and see if the order total
		 * change and order line total changes are there in the output. if
		 * available then set these fields at
		 */

		List<Element> liOAuditAttrTotalAmount = XMLUtils
				.getElementListByXpath(
						inputDocument,
						"/Order/OrderAudit/OrderAuditLevels/OrderAuditLevel[@ModificationLevel='ORDER']/OrderAuditDetails/OrderAuditDetail[@AuditType='OrderHeader']/Attributes/Attribute[@Name='TotalAmount']");

		if (liOAuditAttrTotalAmount.size() == 1) {
			Element eleAttributeNode = liOAuditAttrTotalAmount.get(0);
			BigDecimal bdNewValue = BigDecimal.ZERO;
			BigDecimal bdOldValue = BigDecimal.ZERO;

			String strNewValue = eleAttributeNode
					.getAttribute(LabsConstants.A_NEW_VALUE);
			if (!YFCCommon.isVoid(strNewValue)) {
				bdNewValue = new BigDecimal(strNewValue);
			}
			String strOldValue = eleAttributeNode
					.getAttribute(LabsConstants.A_OLD_VALUE);

			if (!YFCCommon.isVoid(strOldValue)) {
				bdOldValue = new BigDecimal(strOldValue);
			}

			BigDecimal bdChangeInTotal = bdNewValue.subtract(bdOldValue);

			String strChangeInOrderTotal = bdChangeInTotal.setScale(2,
					BigDecimal.ROUND_HALF_UP).toString();
			inputDocument.getDocumentElement().setAttribute(
					"ChangeInOrderTotal", strChangeInOrderTotal);

		}

		// go through the order lines and get the corresponding order audits and
		// get the change in order line total

		List<Element> liOrderLine = XMLUtils.getElementListByXpath(
				inputDocument, LabsConstants.XPATH_ORDER_LINE_RETURN);

		for (Element eleOrderLine : liOrderLine) {

			String strOrderLineKey = eleOrderLine
					.getAttribute(LabsConstants.A_ORDER_LINE_KEY);

			// now using the above order line key get the corresponding order
			// line audit

			List<Element> liOrderLineTotalAmountAudit = XMLUtils
					.getElementListByXpath(
							inputDocument,
							"/Order/OrderAudit/OrderAuditLevels/OrderAuditLevel[@ModificationLevel='ORDER_LINE' and @OrderLineKey='"
									+ strOrderLineKey
									+ "']/OrderAuditDetails/OrderAuditDetail[@AuditType='OrderLine']/Attributes/Attribute[@Name='LineTotal']");

			if (liOrderLineTotalAmountAudit.size() == 1) {
				Element eleAttributeNodeOl = liOrderLineTotalAmountAudit.get(0);
				BigDecimal bdNewValueOl = BigDecimal.ZERO;
				BigDecimal bdOldValueOl = BigDecimal.ZERO;

				String strNewValueOl = eleAttributeNodeOl
						.getAttribute(LabsConstants.A_NEW_VALUE);
				if (!YFCCommon.isVoid(strNewValueOl)) {
					bdNewValueOl = new BigDecimal(strNewValueOl);
				}
				String strOldValueOl = eleAttributeNodeOl
						.getAttribute(LabsConstants.A_OLD_VALUE);

				if (!YFCCommon.isVoid(strOldValueOl)) {
					bdOldValueOl = new BigDecimal(strOldValueOl);
				}

				BigDecimal bdChangeInTotalOl = bdNewValueOl
						.subtract(bdOldValueOl);

				String strChangeInOrderOrderLineTotal = bdChangeInTotalOl
						.setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				eleOrderLine.setAttribute("ChangeInOrderLineTotal",
						strChangeInOrderOrderLineTotal);

			}

		}

		logger.debug("updated document is", inputDocument);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_CANCEL);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderCancelEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 */
	private String orderCreate(Document inputDocument) throws XPathException {

		String strMethodName = "CreateEventPayload.orderCreate()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_CREATED;

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_CREATED);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderCreateEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 * @throws JSONException
	 * @throws IOException
	 * @throws SAXException
	 * @throws ParserConfigurationException
	 * @throws YFSException
	 */
	private String poCancel(YFSEnvironment env, Document inputDocument)
			throws XPathException, JSONException, YFSException,
			ParserConfigurationException, SAXException, IOException {

		String strMethodName = "CreateEventPayload.poCancel()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				"/Order/Extn/@ExtnTenantID");

		if (VERTICALID.equalsIgnoreCase("23")) {
			fetchAndUpdateRefLineID(env, inputDocument);
		}

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_PO_CANCEL);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for POCancelEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 * @throws IOException
	 * @throws SAXException
	 */
	private String orderFulfillmentCreate(YFSEnvironment env,
			Document inputDocument) throws XPathException, SAXException,
			IOException {

		String strMethodName = "CreateEventPayload.orderFulfillmentCreate()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_PO_CREATED;

		if (VERTICALID.equalsIgnoreCase("23")) {
			fetchAndUpdateRefLineID(env, inputDocument);
		}

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_FULFILLMENTORDER_CREATED);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderFulfillmentCreateEvent"
							+ strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/**
	 * 
	 * @param inputDocument
	 * @throws IOException
	 * @throws SAXException
	 * @throws XPathExpressionException
	 */
	private void fetchAndUpdateRefLineID(YFSEnvironment env,
			Document inputDocument) throws SAXException, IOException,
			XPathExpressionException {
		String strMethodName = "CreateEventPayload.fetchAndUpdateRefLineID()";
		logger.beginTimer("Starting " + strMethodName);
		if (LabsCommonUtil.enableFunctionality("FetchRefLineFromText8ForPO")) {
			try {
				String XPATH = null;

				if (EVENT.equalsIgnoreCase("PO_CREATED")) {
					XPATH = "/Order/OrderLines/OrderLine/CustomAttributes";
				} else if (EVENT.equalsIgnoreCase("PO_CANCEL_BACKORDERED")) {
					XPATH = "/Order/OrderLines/OrderLine/CustomAttributes";
				} else if (EVENT.equalsIgnoreCase("PO_SHIPPED")) {
					XPATH = "/Shipment/ShipmentLines/ShipmentLine/OrderLine/CustomAttributes";
				}

				if (!StringUtils.isNullOrEmpty(XPATH)) {
					List<Element> lEleCustomAttribute = XMLUtils
							.getElementListByXpath(inputDocument, XPATH);
					int length = lEleCustomAttribute.size();
					if (length > 0) {
						for (Element eleCustomAttribute : lEleCustomAttribute) {

							String refLineId = eleCustomAttribute
									.getAttribute("ReferenceLineId");
							String text8 = eleCustomAttribute
									.getAttribute("Text8");
							if (StringUtils.isNullOrEmpty(refLineId)
									&& !StringUtils.isNullOrEmpty(text8)) {
								eleCustomAttribute.setAttribute(
										"ReferenceLineId", text8);
							} else if (StringUtils.isNullOrEmpty(refLineId)
									&& StringUtils.isNullOrEmpty(text8)) {
								fetchLineId(env, inputDocument);
							}
						}
					} else if (EVENT.equalsIgnoreCase("PO_CREATED")
							|| EVENT.equalsIgnoreCase("PO_CANCEL_BACKORDERED")) {
						fetchLineId(env, inputDocument);
					}
				}
			} catch (ParserConfigurationException ex) {
				logger.debug("ParserConfigurationException in fetchAndUpdateRefLineID");
			} catch (TransformerException ex) {
				logger.debug("TransformerException in fetchAndUpdateRefLineID");
			}
		}
		logger.endTimer("Exiting Execution of" + strMethodName);
	}

	private void fetchLineId(YFSEnvironment env, Document inputDocument)
			throws ParserConfigurationException, TransformerException,
			SAXException, IOException, XPathExpressionException {
		List<Element> lEleOrderLines = XMLUtils.getElementListByXpath(
				inputDocument, "/Order/OrderLines/OrderLine");
		Document getOrderLineList = null;

		for (Element eleOrderLine : lEleOrderLines) {
			String OLKey = eleOrderLine.getAttribute("ChainedFromOrderLineKey");
			if (getOrderLineList == null) {
				Document getOrderLineListInDoc = XMLUtils
						.createDocument("OrderLine");
				getOrderLineListInDoc.getDocumentElement().setAttribute(
						"OrderHeaderKey",
						eleOrderLine.getAttribute("ChainedFromOrderHeaderKey"));

				Document template = XMLUtils
						.getDocument("<OrderLineList><OrderLine OrderLineKey=''><CustomAttributes ReferenceLineId='' Text8=''/></OrderLine></OrderLineList>");

				getOrderLineList = LabsCommonUtil.invokeAPI(env, template,
						"getOrderLineList", getOrderLineListInDoc);
				logger.debug("getOrderLineList Output is " + getOrderLineList);
			}
			Element matchOrderLine = XMLUtils
					.getElementFromXPath(getOrderLineList,
							"/OrderLineList/OrderLine[@OrderLineKey = '"
									+ OLKey + "']");
			Element customAttributes = null;
			String referenceLineId = null;
			String text8 = null;
			if (matchOrderLine != null)
				customAttributes = (Element) matchOrderLine
						.getElementsByTagName("CustomAttributes").item(0);
			if (customAttributes != null) {
				referenceLineId = customAttributes
						.getAttribute("ReferenceLineId");
				text8 = customAttributes.getAttribute("Text8");
			}
			if (!YFCCommon.isVoid(referenceLineId) || !YFCCommon.isVoid(text8)) {
				Element eleCustomAttr = inputDocument
						.createElement("CustomAttributes");
				if (!YFCCommon.isVoid(referenceLineId)) {
					eleCustomAttr.setAttribute("ReferenceLineId",
							referenceLineId);
				} else {
					eleCustomAttr.setAttribute("ReferenceLineId", text8);
				}
				eleOrderLine.appendChild(eleCustomAttr);
			} else {
				throw LabsHandler.handleException("ERR-REFLINEID",
						"Reference Line Id Missing ");
			}
		}

	}

	/**
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws JSONException
	 * @throws XPathException
	 * @throws TransformerException
	 * @throws ParserConfigurationException
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws IllegalArgumentException
	 */
	private String orderFulfillmentAck(Document inputDocument)
			throws JSONException, XPathException, ParserConfigurationException,
			TransformerException, SecurityException, NoSuchFieldException,
			NoSuchMethodException, IllegalArgumentException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {

		String strMethodName = "CreateEventPayload.orderFulfillmentAck()";
		logger.beginTimer("Starting " + strMethodName);

		Element order, orderAdtLvls, orderAdtLvl, item, extn, orderAudit = null;
		String orderedQty, shipNode, unitOfMeasure, itemId, extnOrderLineType, primeLineNo;

		com.walmart.services.order.common.model.Order orderDTO = new com.walmart.services.order.common.model.Order();
		PurchaseOrder purchaseOrderDTO = new PurchaseOrder();

		List<PurchaseOrder> lPOList = new ArrayList<PurchaseOrder>();
		List<PurchaseOrderLine> lPOLineList = new ArrayList<PurchaseOrderLine>();

		OfferId offerId = null;

		Element eleAck = inputDocument.getDocumentElement();
		NodeList nlAaudit = eleAck
				.getElementsByTagName(EventBusConstants.ORDER_AUDIT);

		if (nlAaudit.getLength() > 0) {
			orderAudit = (Element) nlAaudit.item(0);

			if (orderAudit != null) {

				order = (Element) orderAudit.getElementsByTagName(
						EventBusConstants.ORDER).item(0);
				shipNode = order.getAttribute(LabsConstants.A_SHIP_NODE);

				String sOrderNo = order
						.getAttribute(LabsConstants.A_CUSTOMER_PO_NO);
				Preconditions.checkNotNull(sOrderNo, "OrderNo can't be null");
				orderDTO.setOrderNo(sOrderNo);

				purchaseOrderDTO.setShipNode(shipNode);

				orderAdtLvls = (Element) orderAudit.getElementsByTagName(
						"OrderAuditLevels").item(0);

				if (orderAdtLvls != null) {
					NodeList orderAdtLvl1 = orderAdtLvls
							.getElementsByTagName(LabsConstants.E_ORDER_AUDIT_LEVEL);

					for (int i = 0; i < orderAdtLvl1.getLength(); i++) {
						orderAdtLvl = (Element) orderAdtLvl1.item(i);

						if (orderAdtLvl != null) {
							Element orderLine = (Element) orderAdtLvl
									.getElementsByTagName(
											LabsConstants.E_ORDER_LINE).item(0);
							PurchaseOrderLine purchaseOrderLineDTO = new PurchaseOrderLine();
							Measurement lPOOrderedQty = new Measurement();

							item = (Element) orderAdtLvl.getElementsByTagName(
									LabsConstants.E_ITEM).item(0);
							extn = (Element) orderAdtLvl.getElementsByTagName(
									LabsConstants.E_EXTN).item(0);
							orderedQty = orderLine
									.getAttribute(LabsConstants.A_ORDERED_QUANTITY);
							unitOfMeasure = item.getAttribute("UnitOfMeasure");
							itemId = item.getAttribute(LabsConstants.A_ITEM_ID);
							extnOrderLineType = extn
									.getAttribute(LabsConstants.A_EXTN_ORDER_LINE_TYPE);

							primeLineNo = orderLine
									.getAttribute(LabsConstants.A_PRIME_LINE_NO);

							lPOOrderedQty.setMeasurementValue(new BigDecimal(
									orderedQty));
							lPOOrderedQty.setUnitOfMeasure(UnitOfMeasureEnum
									.fromCode(unitOfMeasure));

							Constructor<OfferId> offerIdContrctr = OfferId.class
									.getDeclaredConstructor(String.class);
							offerIdContrctr.setAccessible(true);
							offerId = (OfferId) offerIdContrctr
									.newInstance(itemId);

							purchaseOrderLineDTO.setOrderedQty(lPOOrderedQty);
							purchaseOrderLineDTO.setPrimeLineNo(Integer
									.parseInt(primeLineNo));
							purchaseOrderLineDTO.setRequestOfferId(offerId);

							if (extnOrderLineType != null
									&& extnOrderLineType != "")
								purchaseOrderDTO
										.setShipNodeType(extnOrderLineType);

							lPOLineList.add(purchaseOrderLineDTO);
						}
					}
				}
			}
		}

		String sPONo = eleAck.getAttribute(EventBusConstants.ORDER_NO);

		String dModifyts = null;
		if (orderAudit != null) {
			dModifyts = orderAudit.getAttribute(EventBusConstants.MODIFYTS);
		} else {
			dModifyts = DateUtils.getCurrentDateISOWithTz();
		}

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_PO_ACK);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_PO_ACK);

		Preconditions.checkNotNull(sPONo, "CustomerPONo can't be null");

		purchaseOrderDTO.setPurchaseOrderNo(sPONo);

		if (!YFCCommon.isVoid(dModifyts))
			orderDTO.setLastModified(new Timestamp(DateUtils
					.getMilliSecs(dModifyts)));

		purchaseOrderDTO.setPurchaseOrderLines(lPOLineList);
		lPOList.add(purchaseOrderDTO);
		orderDTO.setPurchaseOrders(lPOList);

		JSONObject jsonStr = new JSONObject(orderDTO.toOrderJsonString());

		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr.toString();
	}

	/**
	 * this method is for creating the payload for order filled or killed event
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 */
	private String orderFilledOrKilledEvent(Document inputDocument)
			throws XPathException {

		String strMethodName = "orderFilledOrKilledEvent";
		logger.beginTimer("orderFilledOrKilledEvent");

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_FILLED_OR_KILLED;

		inputDocument = LabsEventUtils
				.filterRepeatedOfferIdLines(inputDocument);

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_FILLED_OR_KILLED);

		if (dbMapper == null) {
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderCreateEvent" + strMethodName);
		}
		String jsonStr = LabsDozerMapper.generateMappedJson(Order.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("event data published is ", jsonStr);
		logger.endTimer("orderFilledOrKilledEvent");

		return jsonStr;
	}

	/**
	 * this method is for creating the payload for PO Shipped on Invoice
	 * generation event
	 * 
	 * @param env
	 * @param inputDocument
	 * @throws XPathException
	 * @throws ParserConfigurationException
	 * @throws JSONException
	 * @throws IOException
	 * @throws SAXException
	 * @throws TransformerException
	 */
	private String orderInvoiced(YFSEnvironment env, Document inputDocument)
			throws XPathException, ParserConfigurationException, SAXException,
			IOException, TransformerException, JSONException {
		String strMethodName = "CreateEventPayload.orderInvoiced";
		logger.beginTimer(strMethodName);

		// check the root node and if it's OrderInvoice or OrderInvoiceList

		List<Element> liOrderInvoices = XMLUtils.getElementListByXpath(
				inputDocument, "/OrderInvoiceList/OrderInvoice");

		if (liOrderInvoices.size() > 0) {
			Element eleInput = inputDocument.getDocumentElement();
			String strEventName = eleInput
					.getAttribute(EventBusConstants.A_EVENT);

			Element eleOrderInvoice = liOrderInvoices.get(0);
			inputDocument = XMLUtils.getDocument(XMLUtils
					.getElementXMLString(eleOrderInvoice));
			eleInput = inputDocument.getDocumentElement();
			eleInput.setAttribute(LabsConstants.A_EVENT, strEventName);
		}

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER);
		SCHEMAID = EventBusConstants.SCHEMA_SHIPMENT_INVOICED;

		Element inEle = inputDocument.getDocumentElement();
		Element shipmentEle = (Element) inEle.getElementsByTagName("Shipment")
				.item(0);

		Document shipmentDocument = XMLUtils.getDocument(XMLUtils
				.getElementXMLString(shipmentEle));

		String shipmentJson = poShipped(env, shipmentDocument);

		JSONObject ShipmentInvoiced = new JSONObject(shipmentJson);

		// calculate the total refund amount and set the changeInOrderTotal
		// object

		BigDecimal bdOrderTotalChange = BigDecimal.ZERO;

		if (LabsCommonUtil
				.enableFunctionality("CalculateTaxRefundBeforeOnInvoiceEvent")) {
			String sTotalRefundTaxAmount = inputDocument.getDocumentElement()
					.getAttribute("TotalRefundTaxAmount");
			if (!YFCCommon.isVoid(sTotalRefundTaxAmount)) {
				bdOrderTotalChange = new BigDecimal(sTotalRefundTaxAmount);
				bdOrderTotalChange = bdOrderTotalChange.negate();
			}

		} else {

			List<Element> liLineTaxExtn = XMLUtils
					.getElementListByXpath(
							inputDocument,
							"/OrderInvoice/LineDetails/LineDetail/LineTaxes/LineTax/Extn[@ExtnDeltaTaxAmount<'0']");

			for (Element eleLineTaxExtn : liLineTaxExtn) {
				String strExtnDeltaTaxAmount = eleLineTaxExtn
						.getAttribute(LabsConstants.A_EXTN_DELTA_TAX_AMT);
				BigDecimal bdDeltaTax = new BigDecimal(strExtnDeltaTaxAmount);
				bdOrderTotalChange = bdOrderTotalChange.add(bdDeltaTax);
			}
		}
		JSONObject jsonChangeInOrderTotal = new JSONObject();
		jsonChangeInOrderTotal.put("currencyAmount", bdOrderTotalChange
				.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
		ShipmentInvoiced.put("changeInOrderTotal", jsonChangeInOrderTotal);

		String jsonStr = ShipmentInvoiced.toString();

		logger.debug("event data published is ", jsonStr);
		logger.endTimer(strMethodName);
		return jsonStr;

	}

	/**
	 * Event generation for Adjustments on order post shipment and invoice
	 * 
	 * @param inputDocument
	 * @return
	 * @throws XPathException
	 * @throws Exception
	 */
	private String orderRefund(Document inputDocument) throws XPathException {
		String strMethodName = "CreateEventPayload.orderRefund()";
		logger.beginTimer("Starting " + strMethodName);

		VERTICALID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_VERTICALID_ORDER_INVOICE);
		TENANTID = XMLUtils.getAttributeFromXPath(inputDocument,
				EventBusConstants.XPATH_TENANTID_ORDER_INVOICE);
		SCHEMAID = EventBusConstants.SCHEMA_ORDER_REFUND;

		DozerBeanMapper dbMapper = LabsDozerMapper
				.getDozerMapper(EventBusConstants.MAPPER_ORDER_REFUND);

		if (dbMapper == null)
			throw LabsHandler.handleException(
					LabsConstants.ERROR_FILE_NOT_FOUND_EXCEPTION,
					"Dozer Mapper for OrderCreateEvent" + strMethodName);

		String jsonStr = LabsDozerMapper.generateMappedJson(OrderInvoice.class,
				com.walmart.services.order.common.model.Order.class,
				inputDocument, dbMapper);

		logger.debug("JSON" + jsonStr);
		logger.endTimer("Exiting Execution of" + strMethodName);

		return jsonStr;
	}

	/****
	 * 
	 * @param docOrderDetailsResponse
	 * @param alValues
	 * @throws XPathExpressionException
	 */
	private void parseAndremoveDateTypeId(Document docOrderDetailsResponse,
			List<String> alValues) throws XPathExpressionException {
		/*
		 * Element orderDates =
		 * XMLUtils.getElementFromXPath(docOrderDetailsResponse,
		 * "/OrderList/Order/OrderLines/OrderLine/OrderDates");
		 * removeDateTypeId(orderDates, alValues);
		 * 
		 * orderDates = XMLUtils.getElementFromXPath(docOrderDetailsResponse,
		 * "/OrderList/Order/OrderDates"); removeDateTypeId(orderDates,
		 * alValues); orderDates =
		 * XMLUtils.getElementFromXPath(docOrderDetailsResponse,
		 * "/OrderList/Order/PurchaseOrders");
		 */
		List<Element> orderDates = XMLUtils.getElementsByTagName(
				docOrderDetailsResponse.getDocumentElement(), "OrderDate");

		for (Element orderDate : orderDates) {
			if (!alValues.contains(orderDate
					.getAttribute(LabsConstants.A_DATE_TYPE_ID))) {
				/*
				 * if(LabsPIPConstants.DATE_TYPE_ID_PIP_PAY_BY_DATE.equals(orderDate
				 * .getAttribute(LabsPIPConstants.A_DATE_TYPE_ID))) { Element
				 * pipPaymentMethod =
				 * XMLUtils.getElementFromXPath(docOrderDetailsResponse,
				 * "/OrderList/Order/PaymentMethods/PaymentMethod[@PaymentType='PIP']"
				 * ); if (pipPaymentMethod!=null){
				 * pipPaymentMethod.setAttribute("PIPPayByDate",
				 * orderDate.getAttribute(LabsPIPConstants.A_ACTUAL_DATE)); } }
				 */
				Element parent = (Element) orderDate.getParentNode();
				XMLUtils.removeChild(parent, orderDate);
			}
		}

	}

	/**
	 * 
	 * @param sEnterpriseCode
	 * @param env
	 */
	private List<String> fetchEnumDateTypeIds(String sEnterpriseCode,
			YFSEnvironment env) {

		List<String> alValues = new ArrayList<String>();
		Document docCommonCode = LabsCommonUtil.getCommonCodeListDocument(
				"ETL_ENUM_DATETYPEID", sEnterpriseCode, env);
		List<Element> commonCodeList = XMLUtils
				.getElementsByTagName(docCommonCode.getDocumentElement(),
						LabsConstants.E_COMMON_CODE);
		for (Element commonElement : commonCodeList) {
			alValues.add(commonElement.getAttribute(LabsConstants.A_CODE_VALUE));

		}

		return alValues;
	}

}
