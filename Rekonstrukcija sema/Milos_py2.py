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
                product = {
                    "id": row["product_id"],
                    "type": row.get("product_type", ""),
                    "name": row["product_name"],
                    "brand": {
                        "id": row["brand_id"],
                        "name": row["brand_name"]
                    },
                    "season": row["product_season"],
                    "condition": row["product_condition"],
                    "material": row["product_material"],
                    "color": row["product_color"],
                    "price_usd": row["price_usd"],
                    "state": {
                        "sold": row.get("sold", ""),
                        "reserved": row.get("reserved", ""),
                        "available": row.get("available", "")

                    },
                    "seller": {
                        "earning": row["seller_earning"],
                        "id": row["seller_id"],
                        "country": row["seller_country"]

                    },
                    "shipping": {
                        "buyers_fees": row["buyers_fees"],
                        "warehouse_name": row["warehouse_name"]
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




