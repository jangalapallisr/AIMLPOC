# Databricks notebook source
df = spark.read.format("bigquery").option("viewsEnabled","true").option("materializationDataset","spark_materialization").option("materializationProject","wmt-fin-fcp-uat-ds").option("project","wmt-fin-fcp-dev").option("parentProject","wmt-fin-fcp-dev").option('table',str("wmt_us_fcp_trc_pnl.Waggle_POC1")).load()
df.registerTempTable("Waggle_POC")

data=sqlContext.sql("select DISTINCT FISCAL_YR_NBR,FISCAL_QTR_NBR, COUNT(*) as NUMRECS from Waggle_POC GROUP BY FISCAL_YR_NBR,FISCAL_QTR_NBR ORDER BY FISCAL_YR_NBR,FISCAL_QTR_NBR")
display(data)

# COMMAND ----------

# data=sqlContext.sql("select * from Waggle_POC")
data=sqlContext.sql("select * from Waggle_POC WHERE FISCAL_QTR_NBR=3 AND USD_AMT >0 AND DEPT_NBR IS NOT NULL")
# data=sqlContext.sql("select * from Waggle_POC WHERE USD_AMT >0 AND DEPT_NBR IS NOT NULL")
display(data)

# COMMAND ----------

####** PRODUCE, DAIRY, & GROCERY @WM_WK Level**####
data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,WM_WK_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR IN (81,90,91,92,93,94,97,98)")
####** NON PRODUCE, DAIRY, & GROCERY @WM_WK Level**####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,WM_WK_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR NOT IN (81,90,91,92,93,94,97,98,42,65)")
####** GAS  @WM_WK Level**####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,WM_WK_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR IN (42,65)")
# display(data)

# COMMAND ----------

####** PRODUCE, DAIRY, & GROCERY @WM_MNTH Level**####
#data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR IN (81,90,91,92,93,94,97,98)")
####** NON PRODUCE, DAIRY, & GROCERY @WM_MNTH Level****####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND FISCAL_QTR_NBR=3 AND DEPT_NBR NOT IN (81,90,91,92,93,94,97,98,42,65)")
####** GAS @WM_MNTH Level**####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR IN (42,65)")

# COMMAND ----------

####** NON PRODUCE, DAIRY, & GROCERY @WM_YEARLevel****####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022') AND DEPT_NBR IN (81,90,91,92,93,94,97,98)")
####** NON PRODUCE, DAIRY, & GROCERY @WM_YEARLevel****####
# data = sqlContext.sql(" SELECT FISCAL_YR_NBR,FISCAL_QTR_NBR,FISCAL_PERIOD_NBR,REGION_NBR,STORE_NBR,DEPT_NBR,ACCTG_DEPT_NBR,USD_AMT,NY_GDP_DEFL_KD_ZG, NY_GDP_MKTP_KD_ZG, NY_ADJ_NNTY_PC_CD, NY_TAX_NIND_CD, NY_GNS_ICTR_CD, NY_GNP_PCAP_PP_CD, NY_GNP_MKTP_PP_CD, NY_GDS_TOTL_CD, NY_ADJ_ICTR_GN_ZS, NY_GDP_DISC_CN from Waggle_POC WHERE FISCAL_YR_NBR in ('2021','2022')AND DEPT_NBR NOT IN (81,90,91,92,93,94,97,98)")

# COMMAND ----------

display(data)

# COMMAND ----------

pdf = data.toPandas()
pdf.info()
# pdf.describe()
# pdf.columns

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline
sns.set_style("whitegrid")
# plt.style.use("fivethirtyeight")
# sns.pairplot(pdf)

# COMMAND ----------

# pdf.corr()

# COMMAND ----------

plt.figure(figsize=(20,10))
sns.heatmap(pdf.corr(), annot=True)

# COMMAND ----------

pdf.replace([np.inf, -np.inf], np.nan, inplace=True)
pdf.fillna(999, inplace=True)
X=pdf[['FISCAL_QTR_NBR','FISCAL_PERIOD_NBR','WM_WK_NBR','REGION_NBR','STORE_NBR','DEPT_NBR','NY_GDP_DEFL_KD_ZG','NY_GDP_MKTP_KD_ZG','NY_ADJ_NNTY_PC_CD','NY_GNP_PCAP_PP_CD','NY_GNP_MKTP_PP_CD','NY_ADJ_ICTR_GN_ZS']]
# X=pdf[['FISCAL_QTR_NBR','FISCAL_PERIOD_NBR','WM_WK_NBR','REGION_NBR','STORE_NBR','DEPT_NBR','ACCTG_DEPT_NBR','NY_GDP_DEFL_KD_ZG','NY_GDP_MKTP_KD_ZG','NY_ADJ_NNTY_PC_CD','NY_GNP_PCAP_PP_CD','NY_GNP_MKTP_PP_CD','NY_ADJ_ICTR_GN_ZS']]
# X=pdf[['FISCAL_QTR_NBR','FISCAL_PERIOD_NBR','REGION_NBR','STORE_NBR','DEPT_NBR','NY_GDP_DEFL_KD_ZG','NY_GDP_MKTP_KD_ZG','NY_ADJ_NNTY_PC_CD','NY_GNP_PCAP_PP_CD','NY_GNP_MKTP_PP_CD','NY_GDS_TOTL_CD','NY_ADJ_ICTR_GN_ZS','NY_GDP_DISC_CN']]

y=pdf['USD_AMT']
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=12345)

# COMMAND ----------

def correlation(dt,th):
  col_corr = set()
  corr_mtrx = pdf.corr()
  for i in range(len(corr_mtrx.columns)):
    for j in range(i):
      if abs(corr_mtrx.iloc[i,j]) > th:
        colnm = corr_mtrx.columns[i]
        col_corr.add(colnm)
  return col_corr  
corr_features = correlation(pdf,0.8)
corr_features

# COMMAND ----------

from sklearn import metrics
from sklearn.model_selection import cross_val_score

def cross_val(model):
    pred = cross_val_score(model, X, y, cv=10, scoring='neg_mean_squared_error')
    return pred.mean()

def print_evaluate(true, predicted):  
    mae = metrics.mean_absolute_error(true, predicted)
    mse = metrics.mean_squared_error(true, predicted)
    rmse = np.sqrt(metrics.mean_squared_error(true, predicted))
    r2_square = metrics.r2_score(true, predicted)
    print('MAE:', mae)
    print('MSE:', mse)
    print('RMSE:', rmse)
    print('R2 Square', r2_square)
    print('__________________________________')
    
def evaluate(true, predicted):
    mae = metrics.mean_absolute_error(true, predicted)
    mse = metrics.mean_squared_error(true, predicted)
    rmse = np.sqrt(metrics.mean_squared_error(true, predicted))
    r2_square = metrics.r2_score(true, predicted)
    return mae, mse, rmse, r2_square

# COMMAND ----------

print("**** Linear Regression Model****")
from sklearn.linear_model import LinearRegression

lin_reg = LinearRegression(normalize=True)
lin_reg.fit(X_train,y_train)

print(lin_reg.intercept_)
coeff_df = pd.DataFrame(lin_reg.coef_, X.columns, columns=['Coefficient'])
print(coeff_df)
pred = lin_reg.predict(X_test)
plt.scatter(y_test, pred)

test_pred = lin_reg.predict(X_test)
train_pred = lin_reg.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df = pd.DataFrame(data=[["Linear Regression", *evaluate(y_test, test_pred) , cross_val(LinearRegression())]], 
                          columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', "Cross Validation"])
results_df

# COMMAND ----------

# from sklearn import metrics
# sorted(metrics.SCORERS.keys())

# COMMAND ----------

# # plt.plot(X_test)
# # # plt.plot(X_train)
# # # plt.plot(train_pred)
# # plt.plot(test_pred)
# # plt.legend(['X_test','X_train', 'train_pred', 'test_pred'])
# plt.plot(test_pred)
# plt.xlim(0,10,10)
# plt.xlabel('WM_WK_NBR')
# plt.ylabel('USD_AMT')

# COMMAND ----------

print("**** Robust Regression Model****Random Sample Consensus - RANSAC****")
from sklearn.linear_model import RANSACRegressor

model = RANSACRegressor(base_estimator=LinearRegression(), max_trials=100)
model.fit(X_train, y_train)

test_pred = model.predict(X_test)
train_pred = model.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('====================================')
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Robust Regression", *evaluate(y_test, test_pred) , cross_val(RANSACRegressor())]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', "Cross Validation"])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2

# COMMAND ----------



# COMMAND ----------

print("**** Ridge Regression Model****L2")
from sklearn.linear_model import Ridge

model = Ridge(alpha=100, solver='cholesky', tol=0.0001, random_state=42)
model.fit(X_train, y_train)
pred = model.predict(X_test)

test_pred = model.predict(X_test)
train_pred = model.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('====================================')
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Ridge Regression", *evaluate(y_test, test_pred) , cross_val(Ridge())]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', "Cross Validation"])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2


# COMMAND ----------

print("**** Lasso Regression Model**** L1")
from sklearn.linear_model import Lasso

model = Lasso(alpha=0.1, 
              precompute=True, 
#               warm_start=True, 
              positive=True, 
              selection='random',
              random_state=42)
model.fit(X_train, y_train)

test_pred = model.predict(X_test)
train_pred = model.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('====================================')
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Lasso Regression", *evaluate(y_test, test_pred) , cross_val(Lasso())]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', "Cross Validation"])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2
# plt.plot(test_pred,y_test,)

# COMMAND ----------

print("**** Support Vector Machine ****")
from sklearn.svm import SVR

svm_reg = SVR(kernel='rbf', C=1000000, epsilon=0.001)
svm_reg.fit(X_train, y_train)

test_pred = svm_reg.predict(X_test)
train_pred = svm_reg.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)

print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["SVM Regressor", *evaluate(y_test, test_pred), 0]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', 'Cross Validation'])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2

# COMMAND ----------

print("**** Polynomial Regression Model****")

from sklearn.preprocessing import PolynomialFeatures

poly_reg = PolynomialFeatures(degree=2)

X_train_2_d = poly_reg.fit_transform(X_train)
X_test_2_d = poly_reg.transform(X_test)

lin_reg = LinearRegression(normalize=True)
lin_reg.fit(X_train_2_d,y_train)

test_pred = lin_reg.predict(X_test_2_d)
train_pred = lin_reg.predict(X_train_2_d)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('====================================')
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Polynomail Regression", *evaluate(y_test, test_pred), 0]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', 'Cross Validation'])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2

# COMMAND ----------

print("**** Stochastic Gradient Descent Regression Model****")
from sklearn.linear_model import SGDRegressor

sgd_reg = SGDRegressor(n_iter_no_change=250, penalty=None, eta0=0.0001, max_iter=100000)
sgd_reg.fit(X_train, y_train)

test_pred = sgd_reg.predict(X_test)
train_pred = sgd_reg.predict(X_train)

print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)
print('====================================')
print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Stochastic Gradient Descent", *evaluate(y_test, test_pred), 0]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', 'Cross Validation'])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2

# COMMAND ----------

print("**** Random Forest Regressor ****")
from sklearn.ensemble import RandomForestRegressor

rf_reg = RandomForestRegressor(n_estimators=1000)
rf_reg.fit(X_train, y_train)

test_pred = rf_reg.predict(X_test)
train_pred = rf_reg.predict(X_train)


print('Test set evaluation:\n_____________________________________')
print_evaluate(y_test, test_pred)

print('Train set evaluation:\n_____________________________________')
print_evaluate(y_train, train_pred)

results_df_2 = pd.DataFrame(data=[["Random Forest Regressor", *evaluate(y_test, test_pred), 0]], 
                            columns=['Model', 'MAE', 'MSE', 'RMSE', 'R2 Square', 'Cross Validation'])
results_df = results_df.append(results_df_2, ignore_index=True)
results_df_2

# COMMAND ----------

results_df

# COMMAND ----------

results_df.plot.bar(x='Model', rot=0,figsize=(16, 6))

# COMMAND ----------

# results_df.plot.box()

# COMMAND ----------

print("**** Models Comparison ****")
# results_df.set_index('Model', inplace=True)
results_df['R2 Square'].plot(kind='barh', figsize=(16, 6))

#ax = sns.plot(x="R2 Square", hue="Model", data=results_df, size=50, aspect = 8)
ax = sns.plot(x="R2 Square", hue="Model", data=results_df, size=20, aspect = 10,xlabel='R2 SQR',ylabel='Model', title='R2')
ax.set_xticklabels(rotation=30)
plt.show()

# COMMAND ----------

results_df=results_df.drop(index=[5,6])
results_df 
# = results_df.set_index("Model")


# COMMAND ----------

results_df['R2 Square'].plot(kind='barh', figsize=(16, 6))

# COMMAND ----------

results_df.plot(x='Model',figsize=(12,6))

# COMMAND ----------

# results_df['MAE','Model'].plot(kind='barh', figsize=(12, 8),xlabel='MAE',ylabel='Model',title='MAE HIST')
# # df.plot(xlabel='X Label', ylabel='Y Label', title='Plot Title')
results_df.plot(x='Model',figsize=(12,8))

# COMMAND ----------

results_df['MAE'].plot(x='Model',kind='barh', figsize=(12, 8))

# COMMAND ----------

results_df['MSE'].plot(kind='barh', figsize=(12, 8))

# COMMAND ----------

results_df['RMSE'].plot(kind='barh', figsize=(12, 8))
