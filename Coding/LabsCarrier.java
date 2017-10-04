package com.walmartlabs.services.api.dcc.dto;


import javax.xml.bind.annotation.XmlAccessType;
asdfasd
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
asdfadsf

import com.walmartlabs.services.api.dcc.dto.LabsShipmentCarrierMethodList;
import com.walmartlabs.services.util.base.TransformationUtils;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {

})
@XmlRootElement(name = "LABSCarrier")
public class LabsCarrier {

	@XmlAttribute(name = "CarrierID")    	
	private String carrierID;

	@XmlAttribute(name = "CarrierName")    	
	private String carrierName;

	@XmlAttribute(name =  "TenantID")    	
	private String tenantID;

	@XmlAttribute(name =  "IsActive")    	
	private String isActive;

	@XmlAttribute(name =  "LegacyCarrierID")    	
	private String legacyCarrierID;

	/*@XmlAttribute(name =  "YFSCorporatePersonInfo")    	
	private CorporatePersonInfo YFSCorporatePersonInfo;

	@XmlAttribute(name =  "YFSBillingPersonInfo")    	
	private BillingPersonInfo YFSBillingPersonInfo;

	@XmlAttribute(name =  "YFSContactPersonInfo")    	
	private ContactPersonInfo YFSContactPersonInfo;

	@XmlAttribute(name =  "YFSOrgContactInfo")    	
	private OrgContactInfo YFSOrgContactInfo;*/

	@XmlAttribute(name =  "CarrierTrackingURL")    	
	private String carrierTrackingURL;

	@XmlElement(name =  "LabsShipmentCarrierMethodList") 
	private LabsShipmentCarrierMethodList labsShipmentCarrierMethodList = new LabsShipmentCarrierMethodList();


	public String getTenantID() {
		return tenantID;
	}

	public void setTenantID(String tenantID) {
		this.tenantID = tenantID;
	}

	public String getCarrierID() {
		return carrierID;
	}

	public void setCarrierID(String carrierID) {
		this.carrierID = carrierID;
	}

	public String getCarrierName() {
		return carrierName;
	}

	public void setCarrierName(String carrierName) {
		this.carrierName = carrierName;
	}

	public String getIsActive() {
		return isActive;
	}

	public void setIsActive(String isActive) {
		this.isActive = TransformationUtils.getFlagValueFromBooleanString(isActive);
	}

	public String getLegacyCarrierID() {
		return legacyCarrierID;
	}

	public void setLegacyCarrierID(String legacyCarrierID) {
		this.legacyCarrierID = legacyCarrierID;
	}

	/*public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		if (operation.equalsIgnoreCase(LabsConstants.OPERATION_DELETE))
		{
			this.operation = LabsConstants.OPERATION_DELETE;
		} else if (operation.equalsIgnoreCase(LabsConstants.OPERATION_INSERT))
		{
			this.operation = LabsConstants.OPERATION_CREATE;
		} else 
		{
			this.operation = LabsConstants.OPERATION_MANAGE;
		}
	}*/

	/*public CorporatePersonInfo getYFSCorporatePersonInfo() {
		return YFSCorporatePersonInfo;
	}

	public void setYFSCorporatePersonInfo(CorporatePersonInfo yFSCorporatePersonInfo) {
		YFSCorporatePersonInfo = yFSCorporatePersonInfo;
	}

	public BillingPersonInfo getYFSBillingPersonInfo() {
		return YFSBillingPersonInfo;
	}

	public void setYFSBillingPersonInfo(BillingPersonInfo yFSBillingPersonInfo) {
		YFSBillingPersonInfo = yFSBillingPersonInfo;
	}

	public ContactPersonInfo getYFSContactPersonInfo() {
		return YFSContactPersonInfo;
	}

	public void setYFSContactPersonInfo(ContactPersonInfo yFSContactPersonInfo) {
		YFSContactPersonInfo = yFSContactPersonInfo;
	}

	public OrgContactInfo getYFSOrgContactInfo() {
		return YFSOrgContactInfo;
	}

	public void setYFSOrgContactInfo(OrgContactInfo yFSOrgContactInfo) {
		YFSOrgContactInfo = yFSOrgContactInfo;
	}*/

	public String getCarrierTrackingURL() {
		return carrierTrackingURL;
	}

	public void setCarrierTrackingURL(String carrierTrackingURL) {
		this.carrierTrackingURL = carrierTrackingURL;
	}

	public LabsShipmentCarrierMethodList getLabsShipmentCarrierMethodList() {
		return labsShipmentCarrierMethodList;
	}

	public void setLabsShipmentCarrierMethodList(
			LabsShipmentCarrierMethodList labsShipmentCarrierMethodList) {
		this.labsShipmentCarrierMethodList = labsShipmentCarrierMethodList;
	}

	@Override
	public String toString() {
		return "LabsCarrier [carrierID=" + carrierID + ", carrierName="
				+ carrierName + ", isActive=" + isActive + ", legacyCarrierID="
				+ legacyCarrierID 
				//+ ", operation=" + operation
				//+ ", YFSCorporatePersonInfo=" + YFSCorporatePersonInfo
				//+ ", YFSBillingPersonInfo=" + YFSBillingPersonInfo
				//+ ", YFSContactPersonInfo=" + YFSContactPersonInfo
				//+ ", YFSOrgContactInfo=" + YFSOrgContactInfo
				+ ", carrierTrackingURL=" + carrierTrackingURL
				//+ ", createUserId=" + createUserId + ", modifyUserId="
				//+ modifyUserId 
				+ ", labsShipmentCarrierMethodList="
				+ labsShipmentCarrierMethodList + "]";
	}

	public void setBUSpecificAttributes(String bUId) {
		this.tenantID = TransformationUtils.getTenantIDFromBUCode(bUId);
	}



}
