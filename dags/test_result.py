from util.tw_stock import stock_crawler
import json

sc = stock_crawler()

def get_stock_info_test():
    
    symbol_lst, category_sym_dic =  sc.stock_info()
    assert len(symbol_lst) > 0
    assert symbol_lst[0] == '1101'
    assert symbol_lst[-1] == '9958'
    assert len(list(category_sym_dic.keys())) == 28

def get_each_stock_data_json_test():
    symbol_lst, _ = sc.stock_info()
    with open("dags/data/listed.json", "r",encoding='utf-8') as f:

        stock_data_dict = json.load(f)
    ## remove key column  in the last 
    assert len(list(stock_data_dict.keys())[:-1]) == len(symbol_lst)

def get_top_3_data_by_each_category_json_test():

    _, category_sym_dic =  sc.stock_info()

    for category in category_sym_dic:
        print(category)
        with open(f"dags/data/{category}_top3.json", "r",encoding='utf-8') as f:
            
            category_data_dict = json.load(f)
        ## remove key column  in the last 
        assert len(list(category_data_dict.keys())[:-1]) == 3
        
def test_answer():

    get_stock_info_test()
    get_each_stock_data_json_test()
    
    get_top_3_data_by_each_category_json_test()