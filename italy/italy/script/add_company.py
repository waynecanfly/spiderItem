# -*- coding: utf-8 -*-
import pymysql
import time


conn = pymysql.connect(host="10.100.4.99", port=3306, db="opd_common", user="root", passwd="OPDATA",charset="utf8")
cursor = conn.cursor()
num2 = 470
data = "A.S. Roma@@@A2a@@@Acea@@@Acotel Group@@@Acsm - Agam@@@Aedes@@@Aeffe@@@Albemarle Funds Plc@@@Alby Invest Plc" \
       "@@@Alerion Clean Power@@@Alessia@@@Allianz@@@Ama Ucits Sicav Plc@@@Ambienthesis@@@Amplifon@@@Amundi Index " \
       "Solutions@@@Anima Holding @@@Ansaldo Sts@@@Apsley Fund Icav@@@Aquafil@@@Arnoldo Mondadori Editore@@@Ascopiave" \
       "@@@Assicurazioni Generali@@@Astaldi@@@Astm@@@Atlantia@@@Atomo@@@Autogrill@@@Autostrade Meridionali@@@Avio" \
       "@@@Azimut Holding @@@B&amp;C Speakers @@@Banca Antonveneta@@@Banca Carige@@@Banca Finnat Euramerica@@@Banca Generali" \
       "@@@Banca Ifis@@@Banca Imi@@@Banca Intermobiliare@@@Banca Mediolanum@@@Banca Monte Dei Paschi Di Siena@@@" \
       "Banca Nazionale Del Lavoro@@@Banca Pop. Commercio E Industria@@@Banca Pop.Di Bergamo-Credito Varesino@@@" \
       "Banca Popolare Di Sondrio@@@Banca Profilo@@@Banco Bilbao Vizcaya Argentaria@@@Banco Bpm@@@Banco Di Desio E Della Brianza" \
       "@@@Banco Di Sardegna@@@Bank Of America@@@Bank Of America Merrill Lynch International Limited@@@Barclays Bank Plc@@@" \
       "Barclays Plc@@@Basic Net@@@Bastogi@@@Bb Biotech@@@Be@@@Beghelli@@@Beni Stabili@@@Biancamano@@@Biesse@@@Bioera@@@" \
       "Blackrock Asset Management Deutschland Ag@@@Bnp Paribas@@@Bnp Paribas Easy@@@Bnp Paribas Issuance@@@Boost Issuer@@@" \
       "Borgosesia@@@Bper Banca@@@Brembo@@@Brioschi @@@Brunello Cucinelli@@@Buzzi Unicem@@@C.I.R.@@@Cairo Communication@@@" \
       "Caltagirone@@@Caltagirone Editore@@@Carel Industries@@@Carraro@@@Cattolica Assicurazioni@@@Cembre@@@Cementir Holding@@@" \
       "Centrale Del Latte D'Italia@@@Cerved Group Spa@@@Cft S.P.A.@@@Chl@@@Citigroup@@@Citigroup Funding,  Inc@@@Citigroup Inc" \
       "@@@Class Editori@@@Cnh Industrial@@@Codeis Securities @@@Cofide@@@Coima Res@@@Commerzbank@@@Compam Fund@@@Conafi@@@" \
       "Cover 50 S.P.A. @@@Credit Agricole@@@Credit Suisse@@@Credit Suisse International@@@Credito Emiliano@@@Credito Valtellinese" \
       "@@@Crédit Agricole Cib Finance (Guernsey) Limited @@@Crédit Agricole Corporate And Investment Bank@@@Cs Etf (Ie)@@@" \
       "Cs Etf (Lux)@@@Csp International@@@D'Amico International Shipping@@@Damiani@@@Danieli &amp; C.@@@Datalogic@@@" \
       "Davide Campari - Milano@@@Db Etc Index@@@Db Etc Plc@@@De' Longhi@@@Dea Capital@@@Deutsche Bank@@@Dexia Crediop@@@" \
       "Diaman Sicav@@@Diasorin@@@Digital Bros@@@Digitouch S.P.A.@@@Dobank@@@Dresdner Bank@@@Edison@@@Eems@@@Efficiency Growth Fund Sicav " \
       "@@@Eiger Sicav@@@El.En.@@@Elica@@@Emak@@@Enel@@@Eni@@@Erg@@@Esprinet@@@Etfs Commodity Securities@@@Etfs Equity Securities Limited" \
       "@@@Etfs Foreign Exchange Limited@@@Etfs Hedged Commodity Securities Limited@@@Etfs Hedged Metal Securities Limited@@@" \
       "Etfs Metal Securities@@@Etfs Oil Securities @@@European And Global Investments Ltd@@@European Bank For Reconstruction And Development" \
       "@@@Eurotech @@@Exane Finance Sa.@@@Exprivia@@@Falck Renewables@@@Ferrari@@@Fidia@@@Fiera Milano@@@Fila@@@Fincantieri@@@" \
       "Finecobank@@@Finlabo Investments Sicav@@@Fnm@@@Fullsix@@@Gabetti Property Solutions@@@Gas Plus@@@Ge Capital European Funding Unlimited Company" \
       "@@@Gedi Gruppo Editoriale@@@Gefran@@@Geox@@@Gequity@@@Giglio Group S.P.A.@@@Gima Tt@@@Gold Bullion Securities@@@Goldman Sachs (Jersey) Limited" \
       "@@@Goldman Sachs Group@@@Goldman Sachs International@@@Gruppo Ceramiche Ricchetti@@@Gruppo Mutuionline@@@Gruppo Waste Italia@@@Guala Closures@@@" \
       "Hera @@@I Grandi Viaggi@@@Igd - Immobiliare Grande Distribuzione@@@Il Sole 24ore@@@Ima@@@Immsi@@@Ing Bank@@@Intek Group@@@Interbanca@@@" \
       "Interpump Group@@@Intesa Sanpaolo@@@Invesco Markets@@@Invesco Markets Ii Plc@@@Invesco Markets Iii Plc@@@Invesco Physical Markets@@@Ipi@@@" \
       "Irce@@@Iren@@@Isagro@@@Ishares @@@Ishares (De) I Invag Mit Tgv@@@Ishares (Lux)@@@Ishares Ii@@@Ishares Iii Plc @@@Ishares Iv  @@@Ishares V  " \
       "@@@Ishares Vi@@@Ishares Vii @@@It Way@@@Italgas@@@Italiaonline@@@Italmobiliare@@@Ivs Group@@@J.P. Morgan Structured Fund Management@@@" \
       "J.P. Morgan Structured Products B.V.@@@Jp Morgan Chase Bank@@@Jp Morgan Etfs (Ireland) Icav@@@Juventus Football Club@@@La Doria@@@Landi Renzo" \
       "@@@Legal &amp; General Ucits Etf Plc@@@Lehman Brothers@@@Lehman Brothers Treasury Co. B.V.@@@Leonardo@@@Luxottica Group@@@Lventure Group@@@" \
       "Lyxor Index Fund@@@Lyxor International Asset Management S.A.@@@Macquarie Structured Securities (Europe) Plc@@@Maire Tecnimont@@@Marcolin@@@" \
       "Marr@@@Massimo Zanetti Beverage Group@@@Mediacontech@@@Mediaset@@@Mediobanca@@@Merrill Lynch &amp; Co.@@@Method Investment Sicav@@@Mittel@@@" \
       "Molmed@@@Moncler@@@Mondo Tv@@@Mondo Tv France@@@Monrif@@@Morgan Stanley@@@Morgan Stanley B.V.@@@Mps Merchant@@@Multi Units France@@@" \
       "Multi Units Luxembourg@@@Natixis Structured Issuance@@@Natixis Structured Products Limited@@@Natwest Markets Plc@@@Nb Aurora S.A. Sicaf-Raif" \
       "@@@Netweek @@@Neurosoft@@@New Millennium@@@Nice@@@Noemalife@@@Nova Re@@@Olidata@@@Openjobmetis@@@Orsero Spa@@@Panariagroup Industrie Ceramiche" \
       "@@@Parmalat@@@Pharus Sicav@@@Piaggio &amp; C.@@@Pierrel@@@Pimco Fixed Income Source Etfs@@@Pininfarina@@@Piquadro@@@Pirelli &amp; C. @@@Plc@@@" \
       "Poligrafica S. Faustino@@@Poligrafici Editoriale@@@Poste Italiane@@@Premuda@@@Prima Industrie@@@Prysmian@@@Rai Way@@@Ratti@@@Rcs Mediagroup@@@" \
       "Recordati@@@Reno De Medici@@@Reply@@@Retelit@@@Risanamento@@@Rivage Investment Sas@@@Rosetti Marino@@@S.S. Lazio@@@Sabaf@@@Saes Getters@@@Safilo Group" \
       "@@@Saipem@@@Sal. Oppenheim Jr. &amp; Cie.@@@Salini Impregilo@@@Salvatore Ferragamo@@@Saras@@@Selectra Investments Sicav@@@Seri Industrial@@@" \
       "Servizi Italia@@@Sesa S.P.A.@@@Sg Issuer@@@Sgam Index Sa@@@Silk@@@Snam@@@Societa' Iniziative Autostradali E Servizi - Sias@@@Societe Generale" \
       "@@@Societe Generale Acceptance@@@Societe Generale Effekten @@@Società Di Cartolarizzazione Dei Crediti Inps - S.C.C.I. S.P.A.@@@Sogefi@@@Sol@@@" \
       "Source Csop Markets Plc@@@Ssga Spdr Etfs Europe I @@@Ssga Spdr Etfs Europe Ii Plc@@@Standard Commodities Limited@@@Stefanel@@@Stmicroelectronics" \
       "@@@Structured Invest S.A@@@Sumus Fund@@@Tamburi @@@Tas-Tecnologia Avanzata Dei Sistemi@@@Tbs Group @@@Tcw Funds@@@Technogym@@@Telecom Italia @@@" \
       "Tenaris@@@Terna@@@Ternienergia @@@Tesmec@@@The Royal Bank Of Scotland Group Plc@@@Timeo Neutral Sicav@@@Tinexta@@@Tiscali@@@Tod'S@@@Toscana Aeroporti" \
       "@@@Trevi Group@@@Txt E-Solutions@@@Ubs@@@Ubs (Irl) Etf @@@Ubs Ag London@@@Ubs Etf Sicav@@@Ubs Etfs @@@Unicredit@@@Unicredit Banca Mobiliare@@@" \
       "Unicredit Bank Ag@@@Unieuro@@@Unione Di Banche Italiane@@@Unipol@@@Unipolsai@@@Vg Sicav@@@Vianini@@@Volkswagen @@@Vontobel Financial Products Gmbh" \
       "@@@Wisdomtree Issuer Plc@@@Woodpecker Capital@@@Xtrackers@@@Xtrackers (Ie) Public Limited Company@@@Xtrackers Ii@@@Zignago Vetro@@@Zucchi"


def codefunc():
    global num2
    num2 += 1
    if num2 < 10:
        num = "000" + str(num2)
        code = "ITA1" + num
    elif num2 <= 99:
        num = "00" + str(num2)
        code = "ITA1" + num
    elif num2 <= 999:
        num = "0" + str(num2)
        code = "ITA1" + num
    elif num2 <= 9999:
        code = "ITA1" + str(num2)
    else:
        code = "ITA" + str(num2 + 10000)
    return code


company_list = data.split("@@@")
for temp in company_list:
    sql = "select id from company_data_source where company_name=%s"
    cursor.execute(sql, temp)
    result = cursor.fetchone()
    if result:
        pass
    else:
        code = codefunc()
        is_batch = 1
        download_link = " 	https://www.borsaitaliana.it/borsa/azioni/documenti/societa-quotate/documenti.html?lang=en"
        parameter_data_source = [
            temp,
            download_link,
            is_batch,
            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
            "zx",
            1,
            code,
        ]
        sql_data_source = "insert into company_data_source(company_name,download_link,is_batch," \
                          "gmt_create,user_create,mark,company_id)value(%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql_data_source, parameter_data_source)
        conn.commit()
        print(code)