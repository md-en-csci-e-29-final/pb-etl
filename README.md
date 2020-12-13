[![Build Status](https://travis-ci.com/md-en-csci-e-29-final/pb-etl.svg?branch=master)](https://travis-ci.com/md-en-csci-e-29-final/pb-etl)
[![Maintainability](https://api.codeclimate.com/v1/badges/9d02d0f35d56a317f1c8/maintainability)](https://codeclimate.com/github/md-en-csci-e-29-final/pb-etl/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/9d02d0f35d56a317f1c8/test_coverage)](https://codeclimate.com/github/md-en-csci-e-29-final/pb-etl/test_coverage)
<div style="text-align: center;">

## Final Project
### Python based ETL Process to support auto renewal rate predicting model

Maxim Diatchenko and Evgeny Nenashev



![](harvard.png?style=center)


CSCI E 29 Advanced Python for Data Science
Fall 2020

</div>

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Description of the problem](#description-of-the-problem)
- [Domain life cycle](#domain-life-cycle)
- [Auto-Renew transaction and Auto-Renew grace period](#auto-renew-transaction-and-auto-renew-grace-period)
- [Deletion Probability for individual domain and Auto-Renew Deletion rate](#deletion-probability-for-individual-domain-and-auto-renew-deletion-rate)
- [Attributes (statistics) of internet domain](#attributes-statistics-of-internet-domain)
- [General attributes](#general-attributes)
- [Attributes related to registrar operating activities](#attributes-related-to-registrar-operating-activities)
- [Data Sets](#data-sets)
- [Workflow](#workflow)
- [Model](#model)
- [Model Repository](#model-repository)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Description of the problem  


The goal of this work is to develop an ETL process for a predictive renewal rate model using our new knowledge and classroom experience. We use Django, LUIGI and DASK. In our new ETL process, we implemented logic to asynchronously collect data from various sources (multiple S3 buckets) and transform it into a training dataset and a forecasting dataset. Then, using tensorflow, we created and fitted the neural network model and performed the prediction. The new process stores the result and model in a local repository. We use Django as our API and graphics framework.

[Here is a video presentation of this work](https://youtu.be/ukdVBMfh3Cc)

### Domain life cycle
To get an Internet domain, a client must register it in the registry database through a wholesaler. So the registry has no information regarding the end customer.  The registry company uses the classic subscription business model. Customers buy domains for a period of one to ten years. For the Registry  the “purchase” means a subscription to the DNS service for a specific domain name for a certain period.
The domain can be renewed by customer at any time during subscription period. Nevertheless if the domain expires and is not renewed by the client, the back-office system automatically extends the subscription period by one year. The company charges a standard annual subscription fee. The client is given a grace period of 45 days for a refund and cancellation of a subscription (domain deletion). Though the client can delete (cancel subscription) the domain at any time during subscription period, the most domains' deletes (~ 95%) occur during this 45-day grace period.

### Auto-Renew transaction and Auto-Renew grace period

* Auto-Renew is a financial transaction that renews a subscription for the next year. It is initiated by a batch process every day for all domains with expiration date less than the current date.
* The auto-renewal grace period is 45 days after the auto-renewal transaction. During this period, the registrar can cancel the subscription (delete the domain) with a full refund of the renewal fee. Registrars use this 45-day period to maximize profits from domains that have not already been renewed by their customers (end customers). They try to sell these domains in all kinds of auctions or use domains in the "Pay Per Click" (PPC) business.

### Deletion Probability for individual domain and Auto-Renew Deletion rate
* The subscription business model has two main performance metrics. These are New Registrations and Renewal Rate (ratio of renewed domains to the total number of expiring domains). Financial metrics such as cash flow and revenue are derived from these two KPIs (Key Performance Indicators). An accurate prediction of these KPIs helps predict financial performance. It also helps to adjust the marketing policy if the forecasting financial results do not meet the expectations. The auto-renew deletion rate is used to compute the overall renewal rate
* ***Deletion Probability*** is the probability that a domain will be deleted within the current auto-renew grace period. Sum of all these probabilities over total number of domains in auto-renew state gives the auto-renew deletion rate. Deletion Probability helps to run targeting marketing programs (i.e. renew this particular domain with high deletion probability and get a discount)
* The main goal of this project is finding the Deletion (Renewal) Rate of all domains that are currently in the 45-day grace period. (This is ratio of the amount of deleted domains to the total number of auto-renewed domains.) 
* We do not set the goal to identify every domain that will be deleted. instead we are after Deletion Rate (because it is part of KPI). For example: Say we have a model that finds out individual deletion probabilities for every domain. Then if our target group consists of three domains and individual deletion probability is 33% for each domain in a group, deletion rate is (0.33 * 3)/3 = 0.33. If in fact only one (any) domain from this group is deleted, then actual deletion rate is ⅓ - hence we say model performs well 

### Attributes (statistics) of internet domain
* General attributes  
    * General domain attributes are attributes that are  common to all domains regardless of registrar. For example domain length or domain creation day of the week.

* Attributes related to registrar operating activities 
    * These attributes depend on the registrar's operational activities. For example, a certain registrar deletes domains that they planned to delete only on the 42nd day of the grace period. Thus, the domain of this registrar that is in the grace period for more than 42 days has a very low probability to be deleted. This statement is true only for the particular registrar and most likely be false for all others
### General attributes
| Attribute | Type | Description |
| ------------ | ---------------------------------------------- | ---------------------------------------------- |
| TRANSACTION_ID | Numerical |nique transaction ID (or case ID). Primary key|
| TLD | Categorical | Top-level domain ([wikipedia](https://en.wikipedia.org/wiki/Top-level_domain)) |
| REN | Numerical | Number of prior renewals of domains |
| REGISTRAR_NAME | Categorical | Registrar Name |
| GL_CODE_NAME | Categorical | Registrar accounting (GL) code: USA/Others |
| COUNTRY | Categorical | Registrar Country |
| DOMAIN_LENGTH | Numerical | Length of the domain name without dot-TLD (google.com = length is 6) |
| HISTORY | Categorical | A string that describe renewal history of domain. For example “/CR:2/AR:1/RE:3/AR:1” – domain was created for two years, after that auto-renewed, then prior renewed for 3 years and finally auto-renewed |
| TRANSFERS | Numerical | Number of transfers of domain. Domain can be transferred from one registrar to another. This operation includes mandatory renewal domain for one year and change its registrar |
| TERM_LENGTH | Categorical | The most recent term length of prior renew or create|
| RESTORES | Numerical |  The number of times a domain was restored after deletion. The domain can be restored within 30 days after deletion. This is expensive for the customer, but it brings the domain back if it was deleted by mistake. |
| RES30 | Categorical | This field indicates whether the domain has been restored in the last 30 days |
| REREG | Categorical | This field indicates whether the domain was re-created by another registrar after deletion |
| HD | Categorical | Domain created on weekends; holiday or working day |
| TRAFFIC_SCORE | Numerical | Total domain traffic for the last month normalized to Google.com traffic value |


### Attributes related to registrar operating activities

* QTILE 

Categorical Attribute. Quantile of the domain creation date. Each domain is registered on a specific day (the date the domain was created). “New registrations” are regular time series data. After removing the trends and annual seasonality, we get normally distributed residuals. Thus, the quantile of these residuals is this attribute. For example: a domain that was registered in day with an unusual large number of new registrations is more likely to have different deletion probability than a domain that was registered on a normal date

* DAY_MODIFIER

Nearly every registrar removes unwanted domains on certain days of the 45-day grace period. For example: one registrar always deletes 30% of the total deletion pool on the third day, 60% on the 15th and the rest of the domains on the 40th day, another registrar deletes all domains on the 43rd day. This field displays the overall likelihood of deleting this registrar on a specific date in the grace period. For the first domain in our example on day 39, the value is 0.1 (10%), for the second domain it will be 1 (100%)

* NS_V0, NS_V1, NS_V2

These fields are related to changing the domain name server configuration during 45-day grace period. For example if domain will not be deleted it DNS configuration will be not changed

### Data Sets 

In this work, we use AWS S3 to store and access domain datasets. All fields are masked for security reason

| Dataset | Description |
| ------------ | ---------------------------------------------- | 
|TRN0601|Training dataset created from previous auto-renewal transactions with results.|
|TRN0601_TS|Traffic score for the Training dataset. Normalized monthly traffic value to every domain.|
|TST0601|Data set for prediction. |
|TST0601_TS| Traffic estimation for the forecast dataset. It can be combined with TRN0601_TS, but in real life they are two separate files.|
|TST0601_RSLT|The real result of  the prediction dataset. |

### Workflow
* Loading training data from AWSS3
* Loading traffic score from AWS S3, cleaning and joining it with the training data
* Creating a dataset to normalize data for use in a neural network model
* Creating and fitting a model, saving model into repository 
* Loading dataset for prediction
* Loading traffic score and joining it with the testing data
* Predicting deletion probability and saving result into repository
* Analyzing the predicting data with real result 

### Model

In this work, we use a neural network, but the process is designed in such a way that we can plug multiple models that using different algorithms (SVM, Random Forest, SGD etc.). For example: in our organization we are using Random Forest, it shows the best result for this problem.

### Model Repository

The main purpose of the model repository is to collect in one place different forecasting models and corresponding artifacts (i.e. accuracy graphs). This makes it easier to compare and analyze models. We plan to use leverage Django to continue our future development of this ETL, currently we have just created a basic API