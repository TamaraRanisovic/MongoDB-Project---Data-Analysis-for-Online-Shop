#!/usr/bin/env python
# coding: utf-8

# In[2]:


import csv
import json

def process_csv(file_path, products_file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        with open(products_file_path, mode='w', encoding='utf-8') as products_file:

            products_file.write('[\n')
            first_product = True


            for row in reader:
                product = {
                    "id": row["product_id"],
                    "type": row.get("product_type", ""),
                    "name": row["product_name"],
                    "description": row["product_description"],
                    "keywords": row["product_keywords"],
                    "gender_target": row["product_gender_target"],
                    "category": row["product_category"],
                    "season": row["product_season"],
                    "condition": row["product_condition"],
                    "like_count": row["product_like_count"],
                    "material": row["product_material"],
                    "color": row["product_color"],
                    "price_usd": row["price_usd"],
                    "state": {
                        "sold": row.get("sold", ""),
                        "reserved": row.get("reserved", "")
                    },
                    "seller": {
                        "id": row["seller_id"],
                        "username": row["seller_username"],
                        "num_followers": row["seller_num_followers"]
                    }
                }
                
                if not first_product:
                    products_file.write(',\n')
                json.dump(product, products_file, indent=4)

                
            products_file.write('\n]')


combined_csv_path = "vestiaire.csv"
products_json_path = 'products.json'

process_csv(combined_csv_path, products_json_path)


# In[ ]:





# In[ ]:




