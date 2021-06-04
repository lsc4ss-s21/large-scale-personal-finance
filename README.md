# Large Scale Computing Project: Personal Finance Concerns Analysis

This is the GitHub repository for the final project of MACS 30123 Large Scale Computing.

Author: Jinfei Zhu

# Social Science Question

A Federal Reserve survey finds almost 40% of American adults wouldn't be able to cover a $400 unexpected emergency expense with cash, savings, or a credit card charge that they could quickly pay off. This has drawn my attention and I begin to wonder what are people’s top financial concerns. 

The Personal Finance subreddit is a place where people post their stories and seek for advice, with 14m members and usually around 15k online. Besides, the forum has a filter by flair function, with 13 categories: Auto, Investing, Budgeting, Planning, Credit, Housing Insurance, Retirement, Debt, Saving, Employment, Taxes, Other. I have scraped all posts from this forum with Pushshift API (Reddit API has a limitation of recent 1000 posts) from 2009 to 2020 and store them in csv files by year. 

The raw data is 1.4GB in total. Not that big as it may sound, it still costs my little PC forever to run some counting tasks, so I think it would be great to transfer my computation to cloud. I would upload them to S3, probably would choose to use RDS. I may choose from EC2 and Lambda, or Spark which we will learn in module 3.

# Seriel Computation Bottle-neck

- Because the datasize is very big (for 2020 data, there are more than 180,000 rows), it takes very long time for Pandas do operations on columns, such as cleaning, tokenizing, lemmatizing texts data in my own computer. Pandas to_datetime() method also works slow, which impede me to look at the time distribution of the posts (original data 
- Gemsim Topic Modeling runs very slow 

# Structure of Project
- Collect Data with Pushshift API and upload Data to AWS S3 bucket
- Analyze the number of posts per day with Dask
  - Change UTC to datetime
  - Compare if the distribution of certain flair is not even (for example, tax posts increase in tax season)
- Spark Machine Learni    ng and Spark NLP
  - Predicting high score of Reddit Posts
  - Topic Modeling with posts texts
  - Text summation of posts and answering questions in posts' title










# Last Note: Black Humor of Sparknlp Q&A

Though studied people's discussion, as a researcher, my final goal is to ease people's financial burdens. 

Many titles of Personal Finance Subreddit are questions, so I wonder if the pre-trained `sentence_detector_dl` and `google_t5_small_ssm_nq` model will answer questions properly. However, given the openness of questions from Reddit, which could triger good discussion but don't have a single correct answer, the automatically generated answers sound a little like black humor...

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


# Reference

Dask Metadata: https://docs.dask.org/en/latest/dataframe-design.html#metadata

Dask to_datetime(): https://docs.dask.org/en/latest/dataframe-api.html?highlight=to_datetime#dask.dataframe.to_datetime, https://stackoverflow.com/questions/39584118/dask-dataframe-how-to-convert-column-to-to-datetime
                  
Time Module: https://docs.python.org/3/library/time.html#time.strftime

Spark-nlp:

- Text Summation: https://demo.johnsnowlabs.com/public/TEXT_SUMMARIZATION/

pyspark.ml:

- CountVectorizer: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.CountVectorizer.html
- Topic Modeling: https://github.com/alejandronotario/LDA-Topic-Modeling/blob/master/pySpark/LDA_pySpark_2.ipynb, https://github.com/prrao87/topic-modelling/blob/master/src/notebooks/3_pyspark_lda.ipynb
