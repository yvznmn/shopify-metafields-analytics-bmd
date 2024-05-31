# capstone-project-proposal-yavuz-bmd

My wife owns a business(organicbakeryaustin.com), she bake custom cakes for events in Austin,Texas. 
I have built the platforms for her to sell and display her products in Shopify. We have fulfilled and upcoming orders that needs to be analyzed. My purpose is to build pipelines to run analytics on orders data. This data will come through Shopify Admin APIs, and analayzed in AWS platform. Currently, we have a static website in S3 that is available only for us to see orders details, that is using API Gateways to trigger AWS Lambda functions, to run simple analytics. But the problem is I want to run complex analytics on them, so I need to create a data warehouse and built data pipelines on top of it. That will be my capstone project.

The actual code is in my personal repository. The reason i want to use that is there is already setup prod env, and in use. I want to improve it with the new strategies I learned in this course. Below is the link.

https://github.com/yvznmn/bake_my_day_modules


python3 -m venv venv
source venv/bin/activate


**CONCEPTUAL MODELING**

![alt text](bmd_conceptual_data_modeling.png)
