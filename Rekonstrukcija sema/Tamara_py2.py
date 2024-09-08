#!/usr/bin/env python
# coding: utf-8

# In[2]:


import csv
import json

def process_csv(file_path, products_file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        with open(products_file_path, mode='w', encoding='utf-8') as products_file:


            
            # Initialize JSON writers
            products_file.write('[\n')


            first_product = True


 for row in reader:
                # Create product dictionary
                product = {
                    "id": row["product_id"],
                    "brand": {
                        "id": row["brand_id"],
                        "name": row["brand_name"],
                    },
                    "category": row["product_category"],
                    "condition": row["product_condition"],
                    "like_count": row["product_like_count"],
                    "material": row["product_material"],
                    "price_usd": row["price_usd"],
                    "state": {
                        "sold": row.get("sold", ""),
                        "reserved": row.get("reserved", "")
                    },
                    "seller": {
                        "price": row["seller_price"],
                        "earning": row["seller_earning"],
                        "id": row["seller_id"],
                        "username": row["seller_username"],
                        "country": row["seller_country"],
                        "products_sold": row["seller_products_sold"],
                        "num_products_listed": row["seller_num_products_listed"],
                        "community_rank": row["seller_community_rank"],
                        "num_followers": row["seller_num_followers"],
                        "pass_rate": row["seller_pass_rate"],
                        "badge": row["seller_badge"]
                    }
                }

                
                if not first_product:
                    products_file.write(',\n')
                json.dump(product, products_file, indent=4)
                first_product = False

                
                
            # Finalize JSON arrays
            products_file.write('\n]')


# File paths
combined_csv_path = "vestiaire.csv"
products_json_path = 'products.json'


# Process the CSV file and write to JSON
process_csv(combined_csv_path, products_json_path)


# In[ ]:





# In[ ]:




