一、公司列表更新
早期公司列表下载地址为https://www.sec.gov/Archives/edgar/monthly/，从xbrlrss-2007-01.xml开始解析其中公司信息。其后更新，业务部门确定公司列表从https://www.nasdaq.com/screening/companies-by-industry.aspx获取。详情见代码。

二、公司信息更新
公司信息从sec更新，路径为从搜索页面进入后rss中的company-info标签内容。详情见代码。

三、公司数据更新
早期根据业务要求，只下载各公司xbrl类型的财务报表。其后又根据要求补充对应的html格式的财报，以及非财报数据公告。目前对应的存储位置为Nas1上财报数据：/volume2/data/usa，非财报数据：/volume2/data/usa_announcements，详情见代码。

