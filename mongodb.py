#!/usr/bin/env python
# coding: utf-8

# # Student_Database(MongoDB)

# In[1]:


import pymongo
import pandas as pd


# In[2]:


client = pymongo.MongoClient("mongodb://localhost:27017")


# In[3]:


st_db = client.Students


# In[4]:


st_info = st_db.info


# In[5]:


df = pd.read_json("R:\\students info.json", lines=True)


# In[6]:


df_d = df.to_dict("records")


# In[7]:


st_info.insert_many(df_d)


# ##  Find the student name who scored maximum scores in all (exam, quiz and homework)?
# 

# In[8]:


agr = st_info.aggregate([{"$unwind":"$scores"},{"$group":{"_id":"$_id", "Name":{"$first":"$name"},"Total_Marks":{"$sum":"$scores.score"},}},{"$sort":{"Total_Marks":-1}},{"$limit":1}])
for o in agr:
    print(o)


# ## Find students who scored below average in the exam and pass mark is 40%? 

# In[9]:


av = st_info.aggregate([{"$unwind":"$scores"},{"$match":{"scores.type":"exam","scores.score":{"$gt":40, "$lt":60}}}])


# In[10]:


for f in av:
    print(f)


# ## Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories. 

# In[11]:


x = st_info.aggregate([{"$set":{"scores":{"$arrayToObject":[{"$map":{"input": "$scores","as": "r","in": {"k": "$$r.type", "v": "$$r.score"}}}]}}},{"$project":{"_id":1,"name":1,"Status":{"$cond":{"if": {"$and" : [{"$gte": ["$scores.exam", 40]}, {"$gte": ["$scores.quiz", 40]}, {"$gte": [ "$scores.homework", 40]}]},"then" :"pass","else":"fail"}}}}])
for i in x:
    print(i)


# ## Find the total and average of the exam, quiz and homework and store them in a separate collection. 

# In[12]:


Avg_score = st_db.Avg
agrr = st_info.aggregate([{"$unwind":"$scores"},{"$group":{"_id":"$_id", "Name":{"$first":"$name"},"Total_Marks":{"$sum":"$scores.score"},"Average":{"$avg":"$scores.score"}}},{"$sort":{"Total_Marks":1}}])
for o in agrr:
    st_db.Avg.insert_one(o)


# ## Create a new collection which consists of students who scored below average and above 40% in all the categories.

# In[13]:


below_avg = st_db.Below_Avg

avg = st_info.aggregate([{"$match":{"$expr":{"$and":[{"$gt": [{"$min": "$scores.score"}, 40]},{"$lt": [{"$max": "$scores.score"}, 60]}]}}}])

for i in avg:
    below_avg.insert_one(i)


# ## Create a new collection which consists of students who scored below average and above 40% in all the categories.

# In[14]:


fail_list = st_db.Fail_List
agg = st_info.aggregate([{"$match":{"$expr":{"$lt": [{"$max": "$scores.score"}, 40]}}}])

for i in agg:
    fail_list.insert_one(i)


# ## Create a new collection which consists of students who scored above pass mark in all the categories. 

# In[15]:


pass_list = st_db.Pass_List
agg = st_info.aggregate([{"$match":{"$expr":{"$gt": [{"$min": "$scores.score"}, 40]}}}])

for i in agg:
    pass_list.insert_one(i)

