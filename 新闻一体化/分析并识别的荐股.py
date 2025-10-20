import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import tushare as ts

# 读取三份 Excel
df_sina = pd.read_excel("新浪财经新闻.xlsx")
df_cls = pd.read_excel("财联社新闻.xlsx")
df_ths = pd.read_excel("同花顺新闻.xlsx")

def clean_df(df):
    if 'digest' in df.columns:
        df['text'] = df['digest']
    else:
        df['text'] = df['content']
    df['date'] = pd.to_datetime(df['datetime']).dt.date
    return df[['date', 'text']]

df_sina = clean_df(df_sina)
df_cls = clean_df(df_cls)
df_ths = clean_df(df_ths)

# 合并所有新闻
df_news = pd.concat([df_sina, df_cls, df_ths], ignore_index=True)

# 3. 获取A股公司和行业信息（需要tushare token）
ts.set_token("d0b6869caa73bf448ae153d0c9f80b510749798914ffd05c01a5ea8b")
pro = ts.pro_api()
stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')

#构建映射
stock_dict = {}
industry_dict = {}
for _, row in stock_basic.iterrows():
    ts_code = row['ts_code']
    name = row['name']
    industry = row['industry'] if pd.notna(row['industry']) else "其他"
    stock_dict[name] = ts_code
    stock_dict[ts_code] = ts_code
    industry_dict[name] = industry
    industry_dict[ts_code] = industry
    
    if len(name) > 2:
        industry_dict[name[:2]] = industry

# 识别行业和股票标签
def extract_stock_tag(text, stock_dict):
    res = []
    for key in stock_dict.keys():
        if key in str(text):
            res.append(stock_dict[key])
    return ",".join(set(res)) if res else ""

def extract_industry_tag(text, industry_dict):
    res = []
    for key in industry_dict.keys():
        if key in str(text):
            res.append(industry_dict[key])
    return ",".join(set(res)) if res else "其他"

df_news['stock_tag'] = df_news['text'].apply(lambda x: extract_stock_tag(x, stock_dict))
df_news['industry_tag'] = df_news['text'].apply(lambda x: extract_industry_tag(x, industry_dict))

#用ALBERT模型标注情感
albert_model_path = "local_albert"
tokenizer = AutoTokenizer.from_pretrained(albert_model_path)
albert_model = AutoModelForSequenceClassification.from_pretrained(albert_model_path)
albert_model.eval()

batch_size = 32
labels = []
for i in range(0, len(df_news), batch_size):
    batch_texts = df_news['text'].iloc[i:i+batch_size].tolist()
    inputs = tokenizer(batch_texts, padding=True, truncation=True, max_length=64, return_tensors='pt')
    with torch.no_grad():
        logits = albert_model(**inputs).logits
        batch_labels = torch.argmax(logits, dim=-1).cpu().numpy()
        labels.extend(batch_labels)
df_news['label'] = labels

#输出带标签的新闻表
df_news.to_csv("全部新闻_情感_行业_股票标签.csv", index=False, encoding='utf-8-sig')
print("已输出：全部新闻_情感_行业_股票标签.csv")

#统计每日各行业/股票的正面新闻数量
recommend = df_news[df_news['label'] == 2]  # 2为正面情感
industry_recommend = recommend.groupby(['date','industry_tag']).size().reset_index(name='positive_news_count')
stock_recommend = recommend.groupby(['date','stock_tag']).size().reset_index(name='positive_news_count')

industry_recommend.to_csv("行业推荐信号.csv", index=False, encoding='utf-8-sig')
stock_recommend.to_csv("股票推荐信号.csv", index=False, encoding='utf-8-sig')
print("已输出：行业推荐信号.csv 和 股票推荐信号.csv")
