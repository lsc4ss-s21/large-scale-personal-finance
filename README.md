# Large Scale Computing Project: Personal Finance Concerns Analysis

This is the GitHub repository for the final project of MACS 30123 Large Scale Computing.

Author: Jinfei Zhu

# Social Science Questions

A Federal Reserve survey finds almost 40% of American adults wouldn't be able to cover a $400 unexpected emergency expense with cash, savings, or a credit card charge that they could quickly pay off. This has drawn my attention and I begin to wonder what are people’s top financial concerns. 

The Personal Finance subreddit is a place where people post their stories and seek advice, with 14m members and usually around 15k online. Besides, the forum has a filter by flair function, with 13 categories: Auto, Investing, Budgeting, Planning, Credit, Housing Insurance, Retirement, Debt, Saving, Employment, Taxes, Other. I have scraped all posts from this forum with Pushshift API (Reddit API has a limitation of recent 1000 posts) from 2009 to 2020 and store them in csv files by year. 

My main research question is: what are the topics of personal finance concerns? I am planning to use Topic Modeling to answer this question. Topic Modeling is a two-dimensional clustering method, which assumes a document contains several different topics, and a topic is a distribution over a fixed vocabulary. Though we have thirteen different flairs, will the result of Topic Modeling the same as the human pre-set label? 

Except for this main research question, I am also interested in the time distribution of posting in different flair--maybe we can infer some events from the sudden change in the number of posts. Spark-nlp also has a powerful pre-trained [T5 (Text-To-Text Transfer Transformer)](https://nlp.johnsnowlabs.com/2020/12/21/t5_small_en.html) model for text summarization and question answering. I think that could work on my data, because many posts are relatively long and many posts' titles are in question format. So I wonder if this model could automatically answer questions based on the pre-trained model.

# Seriel Computation Bottlenecks

- Since the scraped Reddit data size is very large (for 2020 data, there are more than 180,000 rows), it takes a very long time for Pandas to do operations on columns, such as cleaning, tokenizing, lemmatizing texts data in my own computer. Pandas `to_datetime()` method also works slow, which impede me to look at the time distribution of the posts (original data contain created time in UTC format)
- Gemsim has been a prevalent topic modeling package for python. However, it runs very slow on my own computer for the large dataset. PySpark ML module also offers an LDA model to fit our data.

# Structure of Project
- Collect Data with Pushshift API and upload Data to AWS S3 bucket: [1_Data_collection_and_upload_to_cloud.ipynb](https://github.com/lsc4ss-s21/final-project-personalfinance/blob/main/1_Data_collection_and_upload_to_cloud.ipynb)
- Analyze the number of posts per day with Dask: [2_Dask_time_distribution.ipynb](https://github.com/lsc4ss-s21/final-project-personalfinance/blob/main/2_Dask_time_distribution.ipynb)
  - Change UTC to datetime
  - Compare if the distribution of certain flair is not even (for example, tax posts increase in tax season)
- Spark Machine Learning and Spark NLP
  - Predicting high score of Reddit Posts [3_Predicting_high_score.ipynb](https://github.com/lsc4ss-s21/final-project-personalfinance/blob/main/3_Predicting_high_score.ipynb)
  - LDA Topic Modeling of posts texts with Spark [4_Pyspark_topic_modeling.ipynb](https://github.com/lsc4ss-s21/final-project-personalfinance/blob/main/4_Pyspark_topic_modeling.ipynb)
  - Text summarization of posts and answering questions in posts' titles [5_Sparknlp_word_summarization_and_question_answering.ipynb](https://github.com/lsc4ss-s21/final-project-personalfinance/blob/main/5_Sparknlp_word_summarization_and_question_answering.ipynb)

# Data
All data used in the notebooks can be found at AWS S3 bucket `large-scale-computing-personal-finance`.

# Result

## Posts Time Distribution
![](https://user-images.githubusercontent.com/72103935/120855976-280c9900-c54d-11eb-8f67-a461ae06d54f.png)
![](https://user-images.githubusercontent.com/72103935/120856454-e7f9e600-c54d-11eb-9a06-57d3275226d0.png)
![](https://user-images.githubusercontent.com/72103935/120856474-f0eab780-c54d-11eb-8d8a-15ff35febc58.png)

- There is a peak for Employment topics. After Christmas and New Year Holiday, there is an increase of the posts in Employment category and it reaches a peak in March and April. In late April or early May, the discussion begins to cool down. This could be the reaction to the Pandemic in the United States.
- In 2020 March, there is a surge in the Investing category. This should be a reflection of the 2020 stock market crash, which was a major and sudden global stock market crash and began on 20 February 2020 and ended on 7 April.
- The heated discussion about Tax occurs at the beginning of the year, from February to Mid-April, and reaches its peak around Tax Day, April 15. There is a second high peak in early July, this is because federal income tax payments due date have been deferred to July 15 because of COVID-19.

## Prediction
![](https://user-images.githubusercontent.com/72103935/120858536-eb42a100-c550-11eb-8987-928c26e9a11e.png)

The Prediction of high score is not very accurate. Perhaps the score of posts is not very informative

## Topic Modeling
![](https://user-images.githubusercontent.com/72103935/120857632-8a669900-c54f-11eb-8b38-f62896907061.png)
I pre-set the number of topics as 13 because there are 13 labels set by the forum moderators manually, and the top words in each topic are shown in the word clouds. There are some overlaps in words. But there are similar topics about the flair, we can see some topics are about auto, insurance, tax, debt, etc.


## Text Summarization
The summarization of posts works well, here is an example:

Original Text:

`
So we have a Jeep Grand Cherokee and we average about 20MPG. We owe about 17.5K on it and our payments are about 420 a month. We spoke about trading it in on a smaller car to get substantial fuel gains ie:a diesel Passat. The ones we see are around 10-11. Would it be worth it to trade in being that the price will go up a little once TT&L is included or should we just keep the GC and pay it off? The fuel mileage is such a plus however the increased maintenance cost might outweigh that. PS: we do put a good bit of mileage on our vehicles and the other vehicle is a truck that we are paying down to get rid of regardless.
`

Text Summarization Result:

```we have a Jeep Grand Cherokee and we average about 20MPG . we owe about 17.5K on it and our payments are about 420 a month . we talked about trading it in on a smaller car to get substantial fuel gains . ```

## Black Humor of Sparknlp Q&A

Though being focusing on people's discussion, as a researcher, my final goal is to ease people's financial burdens. 

Many titles of Personal Finance Subreddit are questions, so I wonder if the pre-trained `sentence_detector_dl` and `google_t5_small_ssm_nq` model will answer questions properly. However, given the openness of questions from Reddit, which could trigger good discussion but don't have a single correct answer, the automatically generated answers sound a little like black humor...

```
Question: Do student loans or credit card debt take precedence?
Answer:	 Over one million students
Question: How does Wageworks work?
Answer:	 sales of ten million units
Question: What to ask for when buying a used car?
Answer:	 car gage
Question: How fast does your credit score actually update?
Answer:	 until you are interrogated
Question: What happens with unused credit card accounts?
Answer:	 bankruptcy
```
The current T5 model is pre-trained by Google, I am thinking of training my own Personal Finance T5 model, but the total corpora may not be large enough to train the model. After all, the discussion and conversation on the forum are very open-ended and in a casual format.

# Reference

Dask Metadata: https://docs.dask.org/en/latest/dataframe-design.html#metadata

Dask to_datetime(): https://docs.dask.org/en/latest/dataframe-api.html?highlight=to_datetime#dask.dataframe.to_datetime, https://stackoverflow.com/questions/39584118/dask-dataframe-how-to-convert-column-to-to-datetime
                  
Time Module: https://docs.python.org/3/library/time.html#time.strftime

Spark-nlp:

- Text Summarization: https://demo.johnsnowlabs.com/public/TEXT_SUMMARIZATION/

pyspark.ml:

- CountVectorizer: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.CountVectorizer.html
- Regexp_replace: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.regexp_replace.html
- RegexTokenizer: https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.ml.feature.RegexTokenizer.html
- Topic Modeling: https://github.com/alejandronotario/LDA-Topic-Modeling/blob/master/pySpark/LDA_pySpark_2.ipynb, https://github.com/prrao87/topic-modelling/blob/master/src/notebooks/3_pyspark_lda.ipynb
