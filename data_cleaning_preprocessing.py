import pandas as pd
from datasets import load_from_disk
import os
import re

#Testing
#categories= ["All_Beauty"]

#testing set
categories= ["All_Beauty", "Amazon_Fashion"]

'''
categories= [
    "All_Beauty", "Amazon_Fashion", "Appliances", "Arts_Crafts_and_Sewing", "Automotive",
    "Baby_Products", "Beauty_and_Personal_Care", "Books", "CDs_and_Vinyl",
    "Cell_Phones_and_Accessories", "Clothing_Shoes_and_Jewelry", "Digital_Music", "Electronics",
    "Gift_Cards", "Grocery_and_Gourmet_Food", "Handmade_Products", "Health_and_Household",
    "Health_and_Personal_Care", "Home_and_Kitchen", "Industrial_and_Scientific", "Kindle_Store",
    "Magazine_Subscriptions", "Movies_and_TV", "Musical_Instruments", "Office_Products",
    "Patio_Lawn_and_Garden", "Pet_Supplies", "Software", "Sports_and_Outdoors",
    "Subscription_Boxes", "Tools_and_Home_Improvement", "Toys_and_Games", "Video_Games", "Unknown"
]
'''

base_path= "dataset/downloads"
files = []

# Need to delete the old dataset before running all categories for space

# Merge on parent asin
for category in categories:
    print(f"Current category: {category}")

    try:
        meta_path = os.path.join(base_path, f"raw_meta_{category}")
        review_path = os.path.join(base_path, f"raw_review_{category}")

        meta = load_from_disk(meta_path)["full"]
        review = load_from_disk(review_path)["full"]
        
        meta_data = meta.to_pandas()
        review_data = review.to_pandas()

        #Testing
        #print("Review data columns before: ", review_data.columns.tolist())
        #print("Review meta columns before: ", meta_data.columns.tolist())

        # Merge on parent asin
        if "parent_asin" in review_data.columns and "parent_asin" in meta_data.columns:
            merged_data = pd.merge(review_data, meta_data, on="parent_asin", how= "inner")
            #print("Merged columns after: ", list(merged_data.columns))
            #print(f"Successful merging of {category}")

            # Handle Invalid / Missing Values

            #Testing
            #Before Handle Invalid / Missing Values
            '''
            print("Before")
            print("Number of rows:", merged_data.shape[0])
            print("Number of rows where star rating is missing or not in [1–5]:", merged_data[~merged_data["rating"].isin([1.0, 2.0, 3.0, 4.0, 5.0])].shape[0])
            print("Number of rows where text (the review body) is empty:", merged_data[merged_data["text"].isna()|(merged_data["text"].str.strip() == "")].shape[0])
            print("Number of rows where brand is unknown:", merged_data.apply(lambda row: True if isinstance(row.get("details"), dict) and "brand" not in row["details"] and (pd.isna(row.get("store")) or row["store"].strip() == "")
                                                          else False, axis=1).sum())
            print("\n") 
            '''
            
            # Drop rows where star rating is missing or not in [1–5].
            if "rating" in merged_data.columns:
                merged_data = merged_data[merged_data["rating"].isin([1.0, 2.0, 3.0, 4.0, 5.0])] #only keep where these are present
            
            # Drop rows if text (the review body) is empty.
            if "text" in merged_data.columns:
                merged_data = merged_data[merged_data["text"].notna()] #dropping nulls
                merged_data = merged_data[merged_data["text"].str.strip() != ""] #if the space had " " dropping that also
            
            # If brand cannot be found in the metadata (e.g., missing in details or store), set brand = “Unknown”.
            if "details" in merged_data.columns or "store" in merged_data.columns:
                merged_data["brand"] = merged_data.apply(lambda row: 
                                                         (row["details"]["brand"]
                                                          if isinstance(row.get("details"), dict) and "brand" in row["details"]
                                                          else row["store"] 
                                                          if pd.notna(row.get("store")) and row["store"].strip()!= ""
                                                          else "Unknown"), 
                                                          axis=1)
            
            
            #Testing
            #After Handle Invalid / Missing Values
            '''
            print("After")
            print("Number of rows:", merged_data.shape[0])
            print("Number of rows where star rating is missing or not in [1–5]:", merged_data[~merged_data["rating"].isin([1.0, 2.0, 3.0, 4.0, 5.0])].shape[0])
            print("Number of rows where text (the review body) is empty:", merged_data[merged_data["text"].isna()|(merged_data["text"].str.strip() == "")].shape[0])
            print("Number of rows where brand is unknown:", merged_data.apply(lambda row: True if isinstance(row.get("details"), dict) and "brand" not in row["details"] and (pd.isna(row.get("store")) or row["store"].strip() == "")
                                                          else False, axis=1).sum())
            print("\n")
            '''

            #Remove Duplicates
            '''
            print("Before removing duplicates")
            print("Number of rows:", merged_data.shape[0])
            print("\n")
            '''
            merged_data = merged_data.drop_duplicates(subset=["user_id", "asin", "text"], keep= "first")

            '''
            print("After removing duplicates")
            print("Number of rows:", merged_data.shape[0])
            print("\n")
            '''

            #Derived Columns

            #Review Length 
            merged_data["review_length"] = merged_data["text"].apply(lambda value: len(re.findall(r'\b\w+\b', value)))

            #Testing
            #print(merged_data.head(2))

            #Year
            #merged_data["timestamp"] = pd.to_datetime(merged_data["timestamp"], unit="ms") 
            merged_data["year"] = pd.to_datetime(merged_data["timestamp"], unit="ms").dt.year

            #Testing
            #print(merged_data[["timestamp", "year"]].head(2))

            #print("Before concatenation")
            #print(f"{category} dataset: ", merged_data.shape)
            #print("\n")

            files.append(merged_data)       
        else:
            print(f"No parent_asin in {category}, skipping")
    except Exception as e:
        print(f"Could not process {category}: {e}")

#Unified Output
cleaned_data= pd.concat(files, ignore_index=True)
print("All categories merging are completed")

#Testing
#print("After concatenation")
#print("Full dataset: ", cleaned_data.shape)
