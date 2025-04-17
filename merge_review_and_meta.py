import pandas as pd
from datasets import load_from_disk
import os

#testing set
#categories= ["All_Beauty", "Amazon_Fashion"]


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

base_path= "dataset/downloads"
files = []

for category in categories:
    print(f"Current category {category}")

    try:
        meta_path = os.path.join(base_path, f"raw_meta_{category}")
        review_path = os.path.join(base_path, f"raw_review_{category}")

        meta = load_from_disk(meta_path)["full"]
        review = load_from_disk(review_path)["full"]
        
        meta_data = meta.to_pandas()
        review_data = review.to_pandas()

        #print("Review data columns before: ", review_data.columns.tolist())
        #print("Review meta columns before: ", meta_data.columns.tolist())

        if "parent_asin" in review_data.columns and "parent_asin" in meta_data.columns:
            merged_data = pd.merge(review_data, meta_data, on="parent_asin", how= "inner")
            #print("Merged columns after: ", list(merged_data.columns))
            print(f"Successful merging of {category}")
            files.append(merged_data)
        else:
            print(f"No parent_asin in {category}, skipping")
    except Exception as e:
        print(f"Could not process {category}: {e}")

print("All categories merging are completed")

