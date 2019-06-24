一、公司列表更新
根据业务要求，解析https://disclosure.edinet-fsa.go.jp/E01EW/BLMainController.jsp?uji.bean=ee.bean.W1E62071.EEW1E62071Bean&uji.verb=W1E62071InitDisplay&TID=W1E62072&PID=currentPage&SESSIONKEY=&lgKbn=1&dflg=0&iflg=0下的EDINET Code List为公司列表入口。详情见代码。


二、公司信息更新
从公司列表解析部分信息字段，另根据要求从东京交易所（http://www2.tse.or.jp/tseHpFront/JJK020010Action.do?Show=Show）通过security_code关联，解析部分公司信息。详情见代码。


三、公司数据更新
早期根据业务要求，从edinet下载公司xbrl类型的财报数据。后期根据要求补充非财报数据及其它格式的财报数据。详情见代码。
