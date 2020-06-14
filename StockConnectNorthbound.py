#%%
from WindPy import w
w.start()
import pandas as pd
import datetime


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def FindData(date, mkt=False, industry=False,df_sh=pd.DataFrame(),df_sz=pd.DataFrame()):
    #
    shdata = w.wset("shstockholdings", "date={}".format(date)).Data
    if shdata!= []:
        wind_code_sh = shdata[0]
        hold_stocks_sh = shdata[2]
        df_sh = pd.DataFrame(hold_stocks_sh, index=wind_code_sh, columns=['stock_holding'])
    #
    szdata = w.wset("szstockholdings", "date={}".format(date)).Data
    if szdata != []:
        wind_code_sz = szdata[0]
        hold_stocks_sz = szdata[2]
        df_sz = pd.DataFrame(hold_stocks_sz, index=wind_code_sz, columns=['stock_holding'])
    #
    if shdata != [] or szdata != []:
        df = pd.concat([df_sh, df_sz], axis=0)
        symbol = df.index.values
        c = symbol[0]
        for i in range(1, len(symbol)):
            if isinstance(symbol[i], str):
                c = c + ',' + symbol[i]
        #
        price = w.wsq(c, "rt_pre_close").Data[0]
        #
        df['price'] = price
        df['market_value'] = df['price'] * df['stock_holding']

        if mkt == True:
            mkt = w.wss(c, "mkt").Data[0]
            df['mkt'] = mkt
            return df[['mkt', 'market_value']]

        if industry == True:
            industry = w.wss(c, "industry_citic", "tradeDate={}; industryType=1".format(date)).Data[0]
            df['industry'] = industry
            return df[['industry', 'market_value']]
    else:
        return None
    #

def DataGroup(date, mkt=False, industry=False):
    #
    if mkt == True:
        data = FindData(date, mkt=True)
        if data is not None:
            data_group_by_mkt = pd.pivot_table(data, columns=['mkt'], aggfunc='sum')
            data_group_by_mkt['合计'] = data_group_by_mkt.sum(axis=1)
            for each in data_group_by_mkt.columns:
                data_group_by_mkt[each + '占比'] = data_group_by_mkt[each] / data_group_by_mkt['合计']
            return data_group_by_mkt
        else:
            return None
    #
    if industry == True:
        data = FindData(date, industry=True)
        if data is not None:
            data_group_by_industry = pd.pivot_table(data, columns=['industry'], aggfunc='sum')
            data_group_by_industry['合计'] = data_group_by_industry.sum(axis=1)
            for each in data_group_by_industry.columns:
                data_group_by_industry[each + '占比'] = data_group_by_industry[each] / data_group_by_industry['合计']
            return data_group_by_industry
        else:
            return None

#mkt 修改结束日期为当月最后一个交易日
startDate = "2016-06-01" #开始日期
endDate = "2020-05-31" #结束日期

monthlydays = w.tdays(startDate, endDate, "Period=M").Data[0]

tradingdays = w.tdays(startDate, endDate, "").Data[0]


df_mkt = pd.DataFrame()
for eachdate in monthlydays:
    data_eachyear = DataGroup(str(eachdate), mkt=True)
    if data_eachyear is not None:
        eachdate_str = datetime.datetime.strftime(eachdate, "%Y-%m-%d")
        data_eachyear.index = [eachdate_str]
        df_mkt = pd.concat([df_mkt, data_eachyear], axis=0)           
df_mkt = df_mkt[['中小企业板占比','主板占比','创业板占比','合计占比','中小企业板','主板','创业板']]   
print(df_mkt)


df_mkt.to_csv('市场板块.csv', encoding='utf-8-sig')


df_industry = pd.DataFrame()
for eachdate in monthlydays:
    data_eachyear = DataGroup(str(eachdate), industry=True)
    #print(data_eachyear)
    if data_eachyear is not None:
        eachdate_str = datetime.datetime.strftime(eachdate, "%Y-%m-%d")
        data_eachyear.index = [eachdate_str]
        df_industry = pd.concat([df_industry, data_eachyear], axis=0)
df_industry = df_industry[["交通运输占比",	"传媒占比",	"农林牧渔占比",	"医药占比",	"商贸零售占比",	"国防军工占比",	"基础化工占比",	"家电占比",	"建材占比",	"建筑占比",	"房地产占比",	"有色金属占比",	"机械占比",	"汽车占比",	"消费者服务占比",	"煤炭占比",	"电力及公用事业占比",	"电力设备及新能源占比",	"电子占比",	"石油石化占比",	"纺织服装占比",	"综合占比",	"计算机占比",	"轻工制造占比",	"通信占比",	"钢铁占比",	"银行占比",	"非银行金融占比",	"食品饮料占比","综合金融占比",	"合计占比",	"交通运输",	"传媒",	"农林牧渔",	"医药",	"商贸零售",	"国防军工",	"基础化工",	"家电",	"建材",	"建筑",	"房地产",	"有色金属",	"机械",	"汽车",	"消费者服务",	"煤炭",	"电力及公用事业",	"电力设备及新能源",	"电子",	"石油石化",	"纺织服装",	"综合",	"计算机",	"轻工制造",	"通信",	"钢铁",	"银行",	"非银行金融",	"食品饮料",	"综合金融",	"合计"]]
print(df_industry)
df_industry.to_csv('行业.csv', encoding='utf-8-sig')
