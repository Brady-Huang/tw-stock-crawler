import requests
from datetime import datetime, timedelta
import json
import random
import pandas as pd
import time
requests.urllib3.disable_warnings()

class stock_crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        }
    
    def stock_info(self):

        print("Get symbol info")
        url = 'https://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
        self.headers['Host'] = 'isin.twse.com.tw'
        resp = requests.get(url,headers=self.headers)
        stock_df = pd.read_html(resp.content,encoding='big5-hkscs')[0]
        stock_df.columns = stock_df.iloc[0].tolist()
        ## find index of  value 1101　台泥 and 上市認購(售)權證 in 有價證券代號及名稱 column. 
        start_index = stock_df.index[stock_df['有價證券代號及名稱']=='1101　台泥'].tolist()[0]
        end_index = stock_df.index[stock_df['有價證券代號及名稱']=='上市認購(售)權證'].tolist()[0]
        ## filter rows between start index and end index
        stock_df = stock_df.loc[start_index:end_index - 1]
        stock_df['有價證券代號及名稱'] = stock_df['有價證券代號及名稱'].apply(lambda x:x.replace('\u3000',' ').split(' ')[0]).tolist()
        stock_df = stock_df[['有價證券代號及名稱','產業別']].copy()
        stock_df.columns = ['symbol','category']
        
        symbol_lst = stock_df['symbol'].tolist()
        category_lst = stock_df['category'].tolist()
        category_sym_dic = {}
        for sym, cat in zip(symbol_lst,category_lst):
            if cat not in category_sym_dic:
                category_sym_dic[cat] = [sym]
            else:
                category_sym_dic[cat].append(sym)
        
        return symbol_lst, category_sym_dic
    
    def individual_stock_crawler_recursion(self,symbol_lst,date,stock_dict):
        
        """
        
        This program iterates each symbol and crawl stock information of each symbol.
        When error occurs, this program uses a list to collect these symbols to run again.
        Use recursion algorithm to repeat this two steps until all data are collected.
        
        """
        self.headers['Host'] = 'www.twse.com.tw'

        print('crawler start')
        session = requests.Session()
        rest_symbol_lst = []
        for symbol in symbol_lst:

            print(symbol)
            symbol_url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?date={date}&stockNo={symbol}"
            for i in range(3):

                try:
                    time.sleep(random.uniform(2,4))

                    res = session.get(symbol_url,headers=self.headers)

                    data_dic = json.loads(res.content)

                    if data_dic['stat'] != 'ok':
                        stock_dict[symbol] = data_dic['stat']
                        break
                    
                    symbol_data_dic = {'data': data_dic['data']}
                    stock_dict[symbol] = symbol_data_dic

                    break
                except OSError as e:
                    print(e)
                    raise ConnectionError

                except Exception as e:
                    print(e)

                    if i == 2:
                        rest_symbol_lst.append(symbol)
        stock_dict['columns'] = data_dic['fields']

        if len(rest_symbol_lst) == 0:

            print('crawler finished')
            return stock_dict
        else:
            return self.individual_stock_crawler_recursion(symbol_lst=rest_symbol_lst,date=date,stock_dict = stock_dict)
    
    def get_each_stock_data(self,symbol_lst):
        today = datetime.today().strftime('%Y%m%d')
        print("Today's date is ",today)

        stock_dict_json = self.individual_stock_crawler_recursion(symbol_lst = symbol_lst,date=today,stock_dict={})

        with open("/usr/local/airflow/dags/data/listed.json", "w",encoding='utf-8') as outfile:
            json.dump(stock_dict_json, outfile,ensure_ascii=False)
        return stock_dict_json
    
    def get_top_3_data_by_category(self,category_sym_dic):
        
        with open("/usr/local/airflow/dags/data/listed.json", "r",encoding='utf-8') as f:
            stock_data_dict = json.load(f)
        columns = stock_data_dict['columns']
        
        
        for category in category_sym_dic:
            print(category)
            
            save_category_json = {}
            
            
            sym_lst = category_sym_dic[category]
            stock_df = pd.DataFrame()
            
            for sym in sym_lst:
                if sym in list(stock_data_dict.keys()) and not isinstance(stock_data_dict[sym], str):
                    temp_sym_data = pd.DataFrame([stock_data_dict[sym]['data'][-1]])
                    temp_sym_data['symbol'] = sym
                    stock_df = stock_df.append(temp_sym_data)

            stock_df.columns = columns + ['symbol']
            stock_df['漲跌價差'] = stock_df['漲跌價差'].apply(lambda x:x.replace('X','')).astype(float)
            stock_df = stock_df.sort_values(by='漲跌價差',ascending=False).head(3)
            
            top3_sym_lst = stock_df['symbol'].tolist()
            for selected_symbol in top3_sym_lst:
                save_category_json[selected_symbol] = stock_data_dict[selected_symbol]['data']
            save_category_json['columns'] = columns
            with open(f"/usr/local/airflow/dags/data/{category}_top3.json", "w",encoding='utf-8') as outfile:
                json.dump(save_category_json, outfile,ensure_ascii=False)
            

        