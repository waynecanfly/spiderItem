# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class GermanyItem(scrapy.Item):
    Name = scrapy.Field()
    wkn = scrapy.Field()
    isin = scrapy.Field()

    Company_Name = scrapy.Field()
    Transparency_Level_on_First_Quotation = scrapy.Field()
    Market_Segment = scrapy.Field()
    Trading_Model = scrapy.Field()
    Country = scrapy.Field()
    Branch = scrapy.Field()
    Share_Type = scrapy.Field()
    Sector = scrapy.Field()
    Subsector = scrapy.Field()
    ISIN = scrapy.Field()
    Reuters_Instrument_Code = scrapy.Field()
    Market_Capitalization = scrapy.Field()
    Number_of_Shares = scrapy.Field()
    Exchange_Symbol = scrapy.Field()
    CCP_Eligible = scrapy.Field()
    Instrument_Type = scrapy.Field()
    Instrument_Subtype = scrapy.Field()
    Instrument_Group = scrapy.Field()
    Trading_Model_Num = scrapy.Field()
    Min_Tradable_Unit = scrapy.Field()
    Max_Spread = scrapy.Field()
    Min_Quote_Size = scrapy.Field()
    Start_Pre_Trading = scrapy.Field()
    End_Post_Trading = scrapy.Field()
    Start_Intraday_Auction = scrapy.Field()
    Constituent_of_the_following_Indices = scrapy.Field()
    About_the_Company = scrapy.Field()
    Address = scrapy.Field()
    Phone = scrapy.Field()
    Fax = scrapy.Field()
    Web = scrapy.Field()
    Contact = scrapy.Field()
    Shareholder_structure_Bearer_shares_without_par = scrapy.Field()
    Established = scrapy.Field()
    Segment = scrapy.Field()
    Market = scrapy.Field()
    End_of_Business_Year = scrapy.Field()
    Accounting_Standard = scrapy.Field()
    Registered_Capital = scrapy.Field()
    Admission_Date = scrapy.Field()
    Executive_Board = scrapy.Field()
    Supervisory_Board = scrapy.Field()
    Exchange = scrapy.Field()

    WKN = scrapy.Field()
    Kontakt = scrapy.Field()
    Email = scrapy.Field()
    Company_Website = scrapy.Field()
    Hauptaktionare = scrapy.Field()
    IndexzugehOrigkeit = scrapy.Field()
    Land = scrapy.Field()
    Market_Capital = scrapy.Field()

    Lead_Broker = scrapy.Field()
    Symbol = scrapy.Field()

    Emittent = scrapy.Field()

    Stammdaten = scrapy.Field()
    Aktionarsstruktur = scrapy.Field()
    Profil = scrapy.Field()
    Management = scrapy.Field()
    Aufsichtsrat = scrapy.Field()
    Aktie_Unternehmen = scrapy.Field()

    pdf_name = scrapy.Field()
    pdf_url = scrapy.Field()
    pdf_path = scrapy.Field()

    country_code = scrapy.Field()
    exchange_market_code = scrapy.Field()
    company_code = scrapy.Field()
    financial_statement_season_type_code = scrapy.Field()
    financial_reporting_standard_code = scrapy.Field()
    disclosure_date = scrapy.Field()
    language_written_code = scrapy.Field()
    doc_type = scrapy.Field()
    doc_source_url = scrapy.Field()
    is_doc_url_direct = scrapy.Field()
    doc_local_path = scrapy.Field()
    doc_downloaded_timestamp = scrapy.Field()
    is_downloaded = scrapy.Field()
    currency_code = scrapy.Field()
    gmt_create = scrapy.Field()
    user_create = scrapy.Field()
    fiscal_year = scrapy.Field()
    report_id = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    origin_mark = scrapy.Field()
    origin_pdf_name = scrapy.Field()

    company_name = scrapy.Field()
    Trading = scrapy.Field()
    company_id = scrapy.Field()
    name_origin = scrapy.Field()
    info_disclosure_id = scrapy.Field()
    country_code_listed = scrapy.Field()

