# Large Scale Computing Project: Personal Finance Concerns Analysis

This is the GitHub repository for the final project of MACS 30123 Large Scale Computing.

Author: Jinfei Zhu

# Structure of Project
- Collect Data with Pushshift API and upload Data to AWS S3 bucket
- Analyze the number of posts per day with Dask
  - Change UTC to datetime
  - Compare if the distribution of certain flair is not even (for example, tax posts increase in tax season)
- Spark Machine Learning and Spark NLP
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
