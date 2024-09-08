








/1 upit/
db.products.aggregate([
  
  {
    $group: {
      _id: {
        condition: "$condition",
        product_type: "$type"
      },
      count: { $sum: 1 }
    }
  },
  
  {
    $sort: {
      "_id.condition": 1,
      count: -1
    }
  },
  
  {
    $group: {
      _id: "$_id.condition",
      top_product_types: {
        $push: {
          product_type: "$_id.product_type",
          count: "$count"
        }
      }
    }
  },
  
  {
    $project: {
      condition: "$_id",
      top_product_types: { $slice: ["$top_product_types", 2] }
    }
  },
 
  {
    $unwind: "$top_product_types"
  },
 
  {
    $lookup: {
      from: "products",
      let: {
        condition: "$condition",
        product_type: "$top_product_types.product_type"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$condition", "$$condition"] },
                { $eq: ["$type", "$$product_type"] }
              ]
            }
          }
        },
        {
          $group: {
            _id: "$color",
            count: { $sum: 1 }
          }
        },
        {
          $sort: { count: -1 }
        },
        {
          $limit: 2
        }
      ],
      as: "top_colors"
    }
  },
  
  {
    $group: {
      _id: "$condition",
      product_types: {
        $push: {
          product_type: "$top_product_types.product_type",
          count: "$top_product_types.count",
          top_colors: "$top_colors"
        }
      }
    }
  },
  
  {
    $project: {
      _id: 0,
      condition: "$_id",
      product_types: 1
    }
  },
  
  {
    $sort: {
      condition: 1 
    }
  }
]);












/2 upit/
db.products.aggregate([
  
  {
    $group: {
      _id: {
        season: "$season",
        material: "$material"
      },
      count: { $sum: 1 },
      total_price_usd: { $sum: "$price_usd" }
    }
  },
  
  {
    $sort: {
      "_id.season": 1,
      count: -1
    }
  },
  
  {
    $group: {
      _id: "$_id.season",
      top_materials: {
        $push: {
          material: "$_id.material",
          count: "$count",
          total_price_usd: "$total_price_usd"
        }
      }
    }
  },
  
  {
    $project: {
      season: "$_id",
      top_materials: { $slice: ["$top_materials", 5] }
    }
  },
 
  {
    $unwind: "$top_materials"
  },
 
  {
    $addFields: {
      "top_materials.avg_price_usd": {
        $divide: ["$top_materials.total_price_usd", "$top_materials.count"]
      }
    }
  },
  
  {
    $group: {
      _id: "$season",
      materials: { $push: "$top_materials" }
    }
  },
 
  {
    $addFields: {
      materials: {
        $map: {
          input: "$materials",
          as: "material",
          in: {
            material: "$$material.material",
            count: "$$material.count",
            avg_price_usd: "$$material.avg_price_usd",
            avg_of_other_4: {
              $avg: {
                $map: {
                  input: {
                    $filter: {
                      input: "$materials",
                      as: "m",
                      cond: { $ne: ["$$m.material", "$$material.material"] }
                    }
                  },
                  as: "other",
                  in: "$$other.avg_price_usd"
                }
              }
            },
            difference: {
              $subtract: [
                "$$material.avg_price_usd",
                {
                  $avg: {
                    $map: {
                      input: {
                        $filter: {
                          input: "$materials",
                          as: "m",
                          cond: { $ne: ["$$m.material", "$$material.material"] }
                        }
                      },
                      as: "other",
                      in: "$$other.avg_price_usd"
                    }
                  }
                }
              ]
            }
          }
        }
      }
    }
  },
  
  {
    $sort: {
      season: 1 
    }
  }
]);








/3 upit/
db.products.aggregate([
 
  {
    $group: {
      _id: {
        country: "$seller.country",
        brand_name: "$brand.name"
      },
      total_seller_earnings: { $sum: "$seller.earning" },
      count: { $sum: 1 }
    }
  },
  
  {
    $group: {
      _id: "$_id.country",
      brands: {
        $push: {
          brand_name: "$_id.brand_name",
          total_seller_earnings: "$total_seller_earnings",
          count: "$count"
        }
      },
      max_count: { $max: "$count" }
    }
  },
  
  {
    $project: {
      country_name: "$_id",
      max_brand: {
        $arrayElemAt: [
          {
            $filter: {
              input: "$brands",
              as: "brand",
              cond: { $eq: ["$$brand.count", "$max_count"] }
            }
          },
          0
        ]
      },
      brands: 1
    }
  },
  
  {
    $project: {
      country_name: 1,
      max_brand_name_for_country: "$max_brand.brand_name",
      max_brand_count_for_country: "$max_brand.count",
      total_seller_earnings_country_max_brand_count_for_country: "$max_brand.total_seller_earnings",
      total_seller_earnings_country_all_other_brands: {
        $reduce: {
          input: "$brands",
          initialValue: 0,
          in: {
            $cond: [
              { $eq: ["$$this.brand_name", "$max_brand.brand_name"] },
              "$$value",
              { $add: ["$$value", "$$this.total_seller_earnings"] }
            ]
          }
        }
      }
    }
  }
]);








/4 upit/
db.products.aggregate([
  
  {
    $match: {
      "shipping.buyers_fees": { $exists: true, $ne: NaN }
    }
  },
 
  {
    $group: {
      _id: "$shipping.warehouse_name",
      total_sold: {
        $sum: { $cond: [{ $eq: ["$state.sold", true] }, 1, 0] }
      },
      total_reserved: {
        $sum: { $cond: [{ $eq: ["$state.reserved", true] }, 1, 0] }
      },
      total_available: {
        $sum: { $cond: [{ $eq: ["$state.available", true] }, 1, 0] }
      },
      avg_buyers_fees: { $avg: "$shipping.buyers_fees" }
    }
  },
  
  {
    $lookup: {
      from: "products",
      let: { warehouseAvg: "$avg_buyers_fees" },
      pipeline: [
        {
          $match: {
            $expr: { $ne: ["$shipping.buyers_fees", NaN] }
          }
        },
        {
          $group: {
            _id: "$seller.country",
            avg_buyers_fees_country: { $avg: "$shipping.buyers_fees" }
          }
        }
      ],
      as: "countries_avg_buyers_fees"
    }
  },
  
  {
    $project: {
      warehouse_name: "$_id",
      total_sold: 1,
      total_reserved: 1,
      total_available: 1,
      avg_buyers_fees: 1,
      countries_less_buyers_fees: {
        $filter: {
          input: "$countries_avg_buyers_fees",
          as: "country",
          cond: { $lt: ["$$country.avg_buyers_fees_country", "$avg_buyers_fees"] }
        }
      }
    }
  },
  
  {
    $unwind: "$countries_less_buyers_fees"
  },
  
  {
    $lookup: {
      from: "products",
      let: { country: "$countries_less_buyers_fees._id", avgBuyersFees: "$countries_less_buyers_fees.avg_buyers_fees_country" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$seller.country", "$$country"] },
                { $ne: ["$shipping.buyers_fees", NaN] }
              ]
            }
          }
        },
        {
          $group: {
            _id: "$type",
            avg_buyers_fees: { $avg: "$shipping.buyers_fees" }
          }
        },
        {
          $project: {
            type: "$_id",
            avg_buyers_fees: 1,
            diff: { $abs: { $subtract: ["$avg_buyers_fees", "$$avgBuyersFees"] } }
          }
        },
        {
          $sort: { diff: 1 }
        },
        {
          $limit: 2
        }
      ],
      as: "nearest_product_types"
    }
  },
  
  {
    $group: {
      _id: "$warehouse_name",
      total_sold: { $first: "$total_sold" },
      total_reserved: { $first: "$total_reserved" },
      total_available: { $first: "$total_available" },
      avg_buyers_fees: { $first: "$avg_buyers_fees" },
      countries_less_buyers_fees: {
        $push: {
          country: "$countries_less_buyers_fees._id",
          avg_buyers_fees_country: "$countries_less_buyers_fees.avg_buyers_fees_country",
          nearest_product_types: "$nearest_product_types"
        }
      }
    }
  },
  
  {
    $project: {
      warehouse_name: "$_id",
      total_sold: 1,
      total_reserved: 1,
      total_available: 1,
      avg_buyers_fees: 1,
      countries_less_buyers_fees: 1
    }
  }
]);



/5 upit/

db.products.aggregate([
{
    $limit: 150000
},
 
  {
    $sort: { price_usd: 1 }
  },
  
  {
    $group: {
      _id: null,
      prices: { $push: "$price_usd" },
      products: { $push: {
        name: "$name",
        type: "$type",
        price_usd: "$price_usd"
      }}
    }
  },
  
  {
    $project: {
      _id: 0,
      ranges: {
        min_q1: { $arrayElemAt: ["$prices", 0] },
        q1_median: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.25] } }] },
        median_q3: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.5] } }] },
        q3_max: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.75] } }] },
        max: { $arrayElemAt: ["$prices", -1] }
      },
      products: "$products"
    }
  },
  
  {
    $project: {
      min_q1: {
        range: "$ranges.min_q1",
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.min_q1"] },
              { $lt: ["$$product.price_usd", "$ranges.q1_median"] }
            ]}
          }
        }
      },
      q1_median: {
        range: {
          min: "$ranges.q1_median",
          max: "$ranges.median_q3"
        },
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.q1_median"] },
              { $lt: ["$$product.price_usd", "$ranges.median_q3"] }
            ]}
          }
        }
      },
      median_q3: {
        range: {
          min: "$ranges.median_q3",
          max: "$ranges.q3_max"
        },
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.median_q3"] },
              { $lt: ["$$product.price_usd", "$ranges.q3_max"] }
            ]}
          }
        }
      },
      q3_max: {
        range: "$ranges.q3_max",
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $gte: ["$$product.price_usd", "$ranges.q3_max"] }
          }
        }
      }
    }
  }
], { allowDiskUse: true });


































db.productsnew.createIndex({ condition: 1, type: 1 });
db.productsnew.createIndex({ color: 1 });



/1 upit sa optimizacijom/
db.productsnew.aggregate([
  
  {
    $group: {
      _id: {
        condition: "$condition",
        product_type: "$type"
      },
      count: { $sum: 1 }
    }
  },
  
  {
    $sort: {
      "_id.condition": 1,
      count: -1
    }
  },
  
  {
    $group: {
      _id: "$_id.condition",
      top_product_types: {
        $push: {
          product_type: "$_id.product_type",
          count: "$count"
        }
      }
    }
  },
  
  {
    $project: {
      condition: "$_id",
      top_product_types: { $slice: ["$top_product_types", 2] }
    }
  },
 
  {
    $unwind: "$top_product_types"
  },
 
  {
    $lookup: {
      from: "products",
      let: {
        condition: "$condition",
        product_type: "$top_product_types.product_type"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$condition", "$$condition"] },
                { $eq: ["$type", "$$product_type"] }
              ]
            }
          }
        },
        {
          $group: {
            _id: "$color",
            count: { $sum: 1 }
          }
        },
        {
          $sort: { count: -1 }
        },
        {
          $limit: 2
        }
      ],
      as: "top_colors"
    }
  },
  
  {
    $group: {
      _id: "$condition",
      product_types: {
        $push: {
          product_type: "$top_product_types.product_type",
          count: "$top_product_types.count",
          top_colors: "$top_colors"
        }
      }
    }
  },
  
  {
    $project: {
      _id: 0,
      condition: "$_id",
      product_types: 1
    }
  },
  
  {
    $sort: {
      condition: 1 
    }
  }
]);












/2 upit sa optimizacijom/
db.productsnew.aggregate([
  
  {
    $group: {
      _id: {
        season: "$season",
        material: "$material"
      },
      count: { $sum: 1 },
      total_price_usd: { $sum: "$price_usd" }
    }
  },
  
  {
    $sort: {
      "_id.season": 1,
      count: -1
    }
  },
  
  {
    $group: {
      _id: "$_id.season",
      top_materials: {
        $push: {
          material: "$_id.material",
          count: "$count",
          total_price_usd: "$total_price_usd"
        }
      }
    }
  },
  
  {
    $project: {
      season: "$_id",
      top_materials: { $slice: ["$top_materials", 5] }
    }
  },
 
  {
    $unwind: "$top_materials"
  },
 
  {
    $addFields: {
      "top_materials.avg_price_usd": {
        $divide: ["$top_materials.total_price_usd", "$top_materials.count"]
      }
    }
  },
  
  {
    $group: {
      _id: "$season",
      materials: { $push: "$top_materials" }
    }
  },
 
  {
    $addFields: {
      materials: {
        $map: {
          input: "$materials",
          as: "material",
          in: {
            material: "$$material.material",
            count: "$$material.count",
            avg_price_usd: "$$material.avg_price_usd",
            avg_of_other_4: {
              $avg: {
                $map: {
                  input: {
                    $filter: {
                      input: "$materials",
                      as: "m",
                      cond: { $ne: ["$$m.material", "$$material.material"] }
                    }
                  },
                  as: "other",
                  in: "$$other.avg_price_usd"
                }
              }
            },
            difference: {
              $subtract: [
                "$$material.avg_price_usd",
                {
                  $avg: {
                    $map: {
                      input: {
                        $filter: {
                          input: "$materials",
                          as: "m",
                          cond: { $ne: ["$$m.material", "$$material.material"] }
                        }
                      },
                      as: "other",
                      in: "$$other.avg_price_usd"
                    }
                  }
                }
              ]
            }
          }
        }
      }
    }
  },
  
  {
    $sort: {
      season: 1 
    }
  }
]);















/3 upit sa optimizacijom/
db.productsnew.aggregate([
 
  {
    $group: {
      _id: {
        country: "$seller.country",
        brand_name: "$brand.name"
      },
      total_seller_earnings: { $sum: "$seller.earning" },
      count: { $sum: 1 }
    }
  },
  
  {
    $group: {
      _id: "$_id.country",
      brands: {
        $push: {
          brand_name: "$_id.brand_name",
          total_seller_earnings: "$total_seller_earnings",
          count: "$count"
        }
      },
      max_count: { $max: "$count" }
    }
  },
  
  {
    $project: {
      country_name: "$_id",
      max_brand: {
        $arrayElemAt: [
          {
            $filter: {
              input: "$brands",
              as: "brand",
              cond: { $eq: ["$$brand.count", "$max_count"] }
            }
          },
          0
        ]
      },
      brands: 1
    }
  },
  
  {
    $project: {
      country_name: 1,
      max_brand_name_for_country: "$max_brand.brand_name",
      max_brand_count_for_country: "$max_brand.count",
      total_seller_earnings_country_max_brand_count_for_country: "$max_brand.total_seller_earnings",
      total_seller_earnings_country_all_other_brands: {
        $reduce: {
          input: "$brands",
          initialValue: 0,
          in: {
            $cond: [
              { $eq: ["$$this.brand_name", "$max_brand.brand_name"] },
              "$$value",
              { $add: ["$$value", "$$this.total_seller_earnings"] }
            ]
          }
        }
      }
    }
  }
]);










db.productsnew.createIndex({ "shipping.warehouse_name": 1, "shipping.buyers_fees": 1 });
db.productsnew.createIndex({ "state.sold": 1, "state.reserved": 1, "state.available": 1 });
db.productsnew.createIndex({ "seller.country": 1 });
db.productsnew.createIndex({ "type": 1 });


/4 upit sa optimizacijom/

db.productsnew.aggregate([
  
  {
    $match: {
      "shipping.buyers_fees": { $exists: true, $ne: NaN }
    }
  },
 
  {
    $group: {
      _id: "$shipping.warehouse_name",
      total_sold: {
        $sum: { $cond: [{ $eq: ["$state.sold", true] }, 1, 0] }
      },
      total_reserved: {
        $sum: { $cond: [{ $eq: ["$state.reserved", true] }, 1, 0] }
      },
      total_available: {
        $sum: { $cond: [{ $eq: ["$state.available", true] }, 1, 0] }
      },
      avg_buyers_fees: { $avg: "$shipping.buyers_fees" }
    }
  },
  
  {
    $lookup: {
      from: "products",
      let: { warehouseAvg: "$avg_buyers_fees" },
      pipeline: [
        {
          $match: {
            $expr: { $ne: ["$shipping.buyers_fees", NaN] }
          }
        },
        {
          $group: {
            _id: "$seller.country",
            avg_buyers_fees_country: { $avg: "$shipping.buyers_fees" }
          }
        }
      ],
      as: "countries_avg_buyers_fees"
    }
  },
  
  {
    $project: {
      warehouse_name: "$_id",
      total_sold: 1,
      total_reserved: 1,
      total_available: 1,
      avg_buyers_fees: 1,
      countries_less_buyers_fees: {
        $filter: {
          input: "$countries_avg_buyers_fees",
          as: "country",
          cond: { $lt: ["$$country.avg_buyers_fees_country", "$avg_buyers_fees"] }
        }
      }
    }
  },
  
  {
    $unwind: "$countries_less_buyers_fees"
  },
  
  {
    $lookup: {
      from: "products",
      let: { country: "$countries_less_buyers_fees._id", avgBuyersFees: "$countries_less_buyers_fees.avg_buyers_fees_country" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$seller.country", "$$country"] },
                { $ne: ["$shipping.buyers_fees", NaN] }
              ]
            }
          }
        },
        {
          $group: {
            _id: "$type",
            avg_buyers_fees: { $avg: "$shipping.buyers_fees" }
          }
        },
        {
          $project: {
            type: "$_id",
            avg_buyers_fees: 1,
            diff: { $abs: { $subtract: ["$avg_buyers_fees", "$$avgBuyersFees"] } }
          }
        },
        {
          $sort: { diff: 1 }
        },
        {
          $limit: 2
        }
      ],
      as: "nearest_product_types"
    }
  },
  
  {
    $group: {
      _id: "$warehouse_name",
      total_sold: { $first: "$total_sold" },
      total_reserved: { $first: "$total_reserved" },
      total_available: { $first: "$total_available" },
      avg_buyers_fees: { $first: "$avg_buyers_fees" },
      countries_less_buyers_fees: {
        $push: {
          country: "$countries_less_buyers_fees._id",
          avg_buyers_fees_country: "$countries_less_buyers_fees.avg_buyers_fees_country",
          nearest_product_types: "$nearest_product_types"
        }
      }
    }
  },
  
  {
    $project: {
      warehouse_name: "$_id",
      total_sold: 1,
      total_reserved: 1,
      total_available: 1,
      avg_buyers_fees: 1,
      countries_less_buyers_fees: 1
    }
  }
]);






/5 upit sa optimizacijom/

db.productsnew.aggregate([
{
    $limit: 150000
},
 
  {
    $sort: { price_usd: 1 }
  },
  
  {
    $group: {
      _id: null,
      prices: { $push: "$price_usd" },
      products: { $push: {
        name: "$name",
        type: "$type",
        price_usd: "$price_usd"
      }}
    }
  },
  
  {
    $project: {
      _id: 0,
      ranges: {
        min_q1: { $arrayElemAt: ["$prices", 0] },
        q1_median: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.25] } }] },
        median_q3: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.5] } }] },
        q3_max: { $arrayElemAt: ["$prices", { $floor: { $multiply: [{ $size: "$prices" }, 0.75] } }] },
        max: { $arrayElemAt: ["$prices", -1] }
      },
      products: "$products"
    }
  },
  
  {
    $project: {
      min_q1: {
        range: "$ranges.min_q1",
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.min_q1"] },
              { $lt: ["$$product.price_usd", "$ranges.q1_median"] }
            ]}
          }
        }
      },
      q1_median: {
        range: {
          min: "$ranges.q1_median",
          max: "$ranges.median_q3"
        },
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.q1_median"] },
              { $lt: ["$$product.price_usd", "$ranges.median_q3"] }
            ]}
          }
        }
      },
      median_q3: {
        range: {
          min: "$ranges.median_q3",
          max: "$ranges.q3_max"
        },
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $and: [
              { $gte: ["$$product.price_usd", "$ranges.median_q3"] },
              { $lt: ["$$product.price_usd", "$ranges.q3_max"] }
            ]}
          }
        }
      },
      q3_max: {
        range: "$ranges.q3_max",
        products: {
          $filter: {
            input: "$products",
            as: "product",
            cond: { $gte: ["$$product.price_usd", "$ranges.q3_max"] }
          }
        }
      }
    }
  }
], { allowDiskUse: true });


