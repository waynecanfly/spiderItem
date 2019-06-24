# -*- coding: utf-8 -*-
import scrapy
import time
import json
import re
from italy.items import ItalyCompanyItem, ItalydetailItem


class ItalyallSpider(scrapy.Spider):
    name = 'sfAll'
    allowed_domains = ['jse.co.za']
    num = 0

    def start_requests(self):
        url = "https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetAllIssuers"
        data = {
            "filterLongName": "",
            "filterType": "Equity Issuer"
        }
        yield scrapy.Request(url, method="POST", body=json.dumps(data), callback=self.parse)

    def parse(self, response):
        data_list = json.loads(response.text)
        for temp in data_list:
            companyItem = ItalyCompanyItem()
            detailItem = ItalydetailItem()
            companyItem["security_code"] = temp["AlphaCode"]
            detailItem["security_code"] = temp["AlphaCode"]
            detailItem["EmailAddress"] = temp["EmailAddress"]
            detailItem["FaxNumber"] = temp["FaxNumber"]
            companyItem["name"] = temp["LongName"]
            companyItem["MasterID"] = temp["MasterID"]
            detailItem["PhysicalAddress"] = temp["PhysicalAddress"]
            detailItem["PostalAddress"] = temp["PostalAddress"]
            companyItem["security_type"] = temp["RoleDescription"]
            companyItem["Status"] = temp["Status"]
            detailItem["TelephoneNumber"] = temp["TelephoneNumber"]
            companyItem["Website"] = temp["Website"]
            detailItem["RegistrationNumber"] = temp["RegistrationNumber"]
            companyItem["gmt_create"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            companyItem["exchange_market_code"] = "JSE"
            companyItem["user_create"] = "zx"
            companyItem["country_code_listed"] = "ZAF"
            profileUrl = "https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetIssuerNatureOfBusiness"
            profileData = {
                "issuerMasterId": str(companyItem["MasterID"])
            }
            yield scrapy.Request(profileUrl, method="POST", body=json.dumps(profileData), callback=self.profileParse,
                                 meta={"companyItem": companyItem, "detailItem": detailItem})

    def profileParse(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        data = json.loads(response.text)
        detailItem["profile"] = data["GetIssuerNatureOfBusinessResult"]
        instrumentsUrl = "https://www.jse.co.za/_vti_bin/JSE/SharesService.svc/GetAllInstrumentsForIssuer"
        instrumentsData = {
            "issuerMasterId": str(companyItem["MasterID"])
        }
        yield scrapy.Request(instrumentsUrl, method="POST", body=json.dumps(instrumentsData), callback=self.instrumentsDataParse,
                             meta={"companyItem": companyItem, "detailItem": detailItem})

    def instrumentsDataParse(self, response):
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        data = json.loads(response.text)["GetAllInstrumentsForIssuerResult"]
        if len(data) != 0:
            data = data[0]
            detailItem["Board"] = data["Board"]
            detailItem["Change"] = data["Change"]
            companyItem["ISIN"] = data["ISIN"]
            companyItem["Industry"] = data["Industry"]
            detailItem["InstrumentType"] = data["InstrumentType"]
            detailItem["ListingDate"] = data["ListingDate"]
            # a = int(re.search("Date\((.{10})", str(detailItem["ListingDate"])).group(1))
            # time_local = time.localtime(a)
            # companyItem["ListingDate"] = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            detailItem["PercentageChange"] = data["PercentageChange"]
            detailItem["MarketCapitalisation"] = data["MarketCapitalisation"]
            detailItem["Price"] = data["Price"]
            companyItem["Sector"] = data["Sector"]
            detailItem["ShortName"] = data["ShortName"]
        else:
            detailItem["Board"] = ""
            detailItem["Change"] = ""
            companyItem["ISIN"] = ""
            companyItem["Industry"] = ""
            detailItem["InstrumentType"] = ""
            detailItem["ListingDate"] = ""
            detailItem["PercentageChange"] = ""
            detailItem["MarketCapitalisation"] = ""
            detailItem["Price"] = ""
            companyItem["Sector"] = ""
            detailItem["ShortName"] = ""
        associateUrl = "https://www.jse.co.za/_vti_bin/JSE/CustomerRoleService.svc/GetIssuerAssociatedRoles"
        associateData = {
            "issuerMasterId": str(companyItem["MasterID"])
        }
        yield scrapy.Request(associateUrl, method="POST", body=json.dumps(associateData), callback=self.associateParse,
                             meta={"companyItem": companyItem, "detailItem": detailItem})

    def associateParse(self, response):
        mesage = []
        companyItem = response.meta["companyItem"]
        detailItem = response.meta["detailItem"]
        data = json.loads(response.text)
        data = data["GetIssuerAssociatedRolesResult"]
        for temp in data:
            mesage.append(temp["RoleDescription"] + ": " + temp["LongName"])
        detailItem["mesage"] = str(mesage)
        yield companyItem
        yield detailItem
