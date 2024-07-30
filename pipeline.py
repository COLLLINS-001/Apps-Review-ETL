import pandas as pd

appsdata = pd.read_csv('apps_data.csv')
reviewdata = pd.read_csv('review_data.csv')

#print(appsdata.head())
#print(reviewdata.head())

#print(appsdata.columns)
#print(reviewdata.columns)

#print(appsdata.shape)
#print(reviewdata.shape)

# A function to extract the data, and print some important information
def extract(filepath):
    data = pd.read_csv(filepath)

    print(f"Here is a little bit of information about the data stored in {filepath}:")
    print(f"\nThere are {data.shape[0]} rows and {data.shape[1]} columns ")

    print("\nThe columns in this DataFrame take the following types: ")
    
    # Print the type of each column
    print(data.dtypes)
    
    # Finally, print a message before returning the DataFrame
    print(f"\nTo view the DataFrame extracted from {filepath}, display the value returned by this function!\n\n")

    return data

appsdata1 = extract('apps_data.csv')
reviewdata1 = extract('review_data.csv')

print(appsdata1)
print(reviewdata1)

# Define a function to transform data
def transform(apps, reviews, category, min_rating, min_reviews):
    # Print statement for observability
    print(f"Transforming data to curate a dataset with all {category} apps and their "
          f"corresponding reviews with a rating of at least {min_rating} and "
          f"{min_reviews} reviews\n")
    
    # Drop any duplicates from both DataFrames (also have the option to do this in-place)
    reviews = reviews.drop_duplicates()
    apps = apps.drop_duplicates(["App"])
    
    # Find all of the apps and reviews in the food and drink category
    subset_apps = apps.loc[apps["Category"] == category, :]
    subset_reviews = reviews.loc[reviews["App"].isin(subset_apps["App"]), ["App", "Sentiment_Polarity"]]
    
    # Aggregate the subset_reviews DataFrame
    aggregated_reviews = subset_reviews.groupby(by="App").mean()
    
    # Join it back to the subset_apps table
    joined_apps_reviews = subset_apps.join(aggregated_reviews, on="App", how="left")
    
    # Keep only the needed columns
    filtered_apps_reviews = joined_apps_reviews.loc[:, ["App", "Rating", "Reviews", "Installs", "Sentiment_Polarity"]]

     # Convert reviews, keep only values with an average rating of at least 4 stars, and at least 1000 reviews
    filtered_apps_reviews = filtered_apps_reviews.astype({"Reviews": "int32"})
    top_apps = filtered_apps_reviews.loc[(filtered_apps_reviews["Rating"] > min_rating) & (filtered_apps_reviews["Reviews"] > min_reviews), :]
    
    # Sort the top apps, replace NaN with 0, reset the index (drop, inplace)
    top_apps.sort_values(by=["Rating", "Reviews"], ascending=False, inplace=True)
    top_apps.reset_index(drop=True, inplace=True)
     
    # Persist this DataFrame as top_apps.csv file
    top_apps.to_csv("top_apps1.csv")
    
    print(f"The transformed DataFrame, which includes {top_apps.shape[0]} rows "
          f"and {top_apps.shape[1]} columns has been persisted, and will now be "
          f"returned")
    
    # Return the transformed DataFrame
    return top_apps

# Call the function
top_apps_data = transform(
    apps=appsdata1,
    reviews=reviewdata1,
    category="FOOD_AND_DRINK",
    min_rating=4.0,
    min_reviews=1000
)

# Show
print(top_apps_data)

import sqlite3

def load(dataframe, database_name, table_name):

    # Create a connection object
    conn = sqlite3.connect(database_name)

    # Write the data to the specified table (table_name)
    dataframe.to_sql(name= table_name , con= conn , if_exists='replace', index=False)
    print("Original DataFrame has been loaded to sqlite\n")

    # Read the data, and return the result (it is to be used)
    loaded_df = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=conn)
    print("The loaded DataFrame has been read from sqlite for validation\n")

    try:
        assert dataframe.shape == loaded_df.shape
        print(f"Success! The data in the {table_name} table have successfully been "
              f"loaded and validated")

    except AssertionError:
        print("DataFrame shape is not consistent before and after loading. Take a closer look!")


# Call the function

load(
    dataframe=top_apps_data,
    database_name='apps_research',
    table_name='top_apps'
)
