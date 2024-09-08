// indeksi

db.products.createIndex({ "seller.country": 1, "seller.pass_rate": 1 });
db.products.createIndex({ "material": 1 });
db.products.createIndex({ "seller.username": 1 });
db.products.createIndex({ "seller.products_sold": 1 });
db.products.createIndex({ "seller.badge": 1 });
db.products.createIndex({ "seller.earning": 1 });







// 1. upit

db.products.aggregate([
	{
		$match: {
			"category": "Women Clothing",
			"seller.badge": "Expert"
		}
	},
    {
        $group: {
            _id: "$seller.id",
            username: { $first: "$seller.username" },
            num_followers: { $max: { $toDouble: "$seller.num_followers" } },
            brands: { $addToSet: "$brand.name" }, 
            totalProductsSold: { $sum: 1 } 
        }
    },
    {
        $sort: { num_followers: -1 }
    },
    {
        $limit: 1
    },
    {
        $lookup: {
            from: "products",
            let: { topSellerId: "$_id" },
            pipeline: [
                {
                    $match: {
                        $expr: { $eq: ["$seller.id", "$$topSellerId"] }
                    }
                },
                {
                    $group: {
                        _id: "$brand.name",
                        totalSold: { $sum: 1 }
                    }
                },
                {
                    $sort: { totalSold: -1 }
                },
                { $limit: 5 }
            ],
            as: "topSellerBrands"
        }
    },
    {
        $unwind: "$topSellerBrands"
    },
    {
        $project: {
            _id: 0,
            username: 1,
            num_followers: 1,
            brand: "$topSellerBrands._id",
            totalSold: "$topSellerBrands.totalSold"
        }
    },
    {
        $group: {
            _id: "$username",
            username: { $first: "$username" },
            num_followers: { $first: "$num_followers" },
            topSellerBrands: { $push: { brand: "$brand", totalSold: "$totalSold" } }
        }
    },
    {
        $project: {
            _id: 0,
            seller: {
                username: "$username",
                num_followers: "$num_followers",
                brands: "$topSellerBrands"            
            }
        }
    }
]);




// 2. upit
db.products.aggregate([
    {
        $addFields: {
            "price_usd_numeric": { $toDouble: "$price_usd" },
            "num_products_listed_numeric": { $toDouble: "$seller.num_products_listed" },
            "products_sold_numeric": { $toDouble: "$seller.products_sold" },
            
        }
    },
    {
        $match: {
            "price_usd_numeric": { $lte: 1000 },
            $or: [
            { "num_products_listed_numeric": { $gte: 50 } },
            { "products_sold_numeric":  { $gte: 25 } } ,
            
        ]
        }
    },
    {
        $group: {
            _id: { brand: "$brand.name", seller: "$seller.id" },
            totalEarnings: { $sum: { $toDouble: "$seller.earning" } }
        }
    },
    {
        $group: {
            _id: "$_id.brand",
            averageEarnings: { $avg: "$totalEarnings" }
        }
    },
    {
        $project: {
            _id: 0, 
            brand: "$_id", 
            averageEarnings: { $round: ["$averageEarnings", 2] }
        }
    },
    {
        $sort: { averageEarnings: -1 }
    },
    {
        $limit: 10
    }
]);



// 3. upit
db.products.aggregate([
    {
        $addFields: {
            "products_sold": { $toDouble: "$seller.products_sold" },
            "earning_numeric": { $toDouble: "$seller.earning" }
        }
    },
    {
        $group: {
            _id: "$seller.id",
            sellerUsername: { $first: "$seller.username" },
            productsSold: { $max: "$products_sold" },
            badge: { $first: "$seller.badge" }
        }
    },
    {
        $sort: { productsSold: -1 }
    },
    {
        $group: {
            _id: "$badge",
            topSellers: { $push: { sellerUsername: "$sellerUsername", productsSold: "$productsSold" } }
        }
    },
    {
        $project: {
            topSellers: { $slice: ["$topSellers", 3] }
        }
    },
    {
        $lookup: {
            from: "products",
            let: { badge: "$_id" },
            pipeline: [
                {
                    $match: {
                        $expr: { $eq: ["$seller.badge", "$$badge"] }
                    }
                },
                {
                    $addFields: {
                        "earning_numeric": { $toDouble: "$seller.earning" }
                    }
                },
                {
                    $group: {
                        _id: "$seller.badge",
                        minEarnings: { $min: "$earning_numeric" },
                        maxEarnings: { $max: "$earning_numeric" },
                        avgEarnings: { $avg: "$earning_numeric" },
                        count: { $sum: 1 }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        badge: "$_id",
                        minEarnings: { $round: ["$minEarnings", 2] },
                        maxEarnings: { $round: ["$maxEarnings", 2] },
                        avgEarnings: { $round: ["$avgEarnings", 2] },
                        count: 1
                    }
                }
            ],
            as: "earningsData"
        }
    },
    {
        $unwind: "$earningsData"
    },
    {
        $project: {
            _id: 0,
            badge: "$_id",
            minEarnings: "$earningsData.minEarnings",
            maxEarnings: "$earningsData.maxEarnings",
            avgEarnings: "$earningsData.avgEarnings",
            count: "$earningsData.count",
            topSellers: 1
        }
    }
]);




// 4. upit
db.products.aggregate([
    {
        $addFields: {
            earning_numeric: { $toDouble: "$seller.earning" },
            like_count_numeric: { $toDouble: "$like_count" },
            num_followers_numeric: { $toDouble: "$seller.num_followers" }
        }
    },
    {
        $group: {
            _id: {
                country: '$seller.country',
                seller_id: '$seller.id'
            },
            username: { $first: '$seller.username' },
            totalEarnings: { $sum: "$earning_numeric" },
            totalLikes: { $sum: "$like_count_numeric" },
            numFollowers: { $first: "$num_followers_numeric" },
            totalProducts: { $sum: 1 } 
        }
    },
    {
        $addFields: {
            avgLikesPerProduct: {
                $cond: {
                    if: { $eq: ["$totalProducts", 0] },
                    then: 0,
                    else: { $divide: ["$totalLikes", "$totalProducts"] }
                }
            },
            likesToFollowersRatio: {
                $cond: {
                    if: { $eq: ["$numFollowers", 0] },
                    then: 0,
                    else: { $multiply: [{ $divide: [{ $cond: {
                        if: { $eq: ["$totalProducts", 0] },
                        then: 0,
                        else: { $divide: ["$totalLikes", "$totalProducts"] }
                    }}, "$numFollowers"] }, 100] }
                }
            }
        }
    },
    {
        $sort: { "totalEarnings": -1, "username": 1 } 
    },
    {
        $group: {
            _id: "$_id.country",
            country: { $first: "$_id.country" },
            rankByEarnings: {
                $push: {
                    username: "$username",
                    totalEarnings: "$totalEarnings",
                    avgLikesPerProduct: "$avgLikesPerProduct",
                    likesToFollowersRatio: "$likesToFollowersRatio"
                }
            }
        }
    },
    {
        $unwind: { path: "$rankByEarnings", includeArrayIndex: "rank" }
    },
    {
        $addFields: {
            "rankByEarnings.rank": { $add: ["$rank", 1] } 
        }
    },

    {
        $group: {
            _id: "$country",
            country: { $first: "$country" },
            rankByEarnings: { $push: "$rankByEarnings" }
        }
    },
    {
        $sort: { "country": 1 } 
    },
    {
        $project: {
            _id: 0, 
            country: 1,
            rankByEarnings: 1 
        }
    }
], { allowDiskUse: true });



// 5. upit

db.products.aggregate([
  {
    $addFields: {
      seller_price: { $toDouble: "$seller.price" },
      pass_rate: { $toDouble: "$seller.pass_rate" }
    }
  },
{
    $match: {
      $or: [
        { "seller.country": "Australia" },
        { "seller.country": "United Kingdom" },
        { "seller.country": "United States" }
      ],
      "pass_rate": { $gt: 85 }
    }
  },


  {
    $group: {
      _id: {
        material: "$material",
        seller: "$seller.username"
      },
      avgPrice: { $avg: "$seller_price" },
      pass_rate: {$max: "$pass_rate"},
      seller_country: {$first: "$seller.country"}
    }
  },
  {
    $group: {
      _id: "$_id.material",
      sellers: {
        $push: {
          seller: "$_id.seller",
          avgPrice: "$avgPrice",
          pass_rate: "$pass_rate",
          seller_country: "$seller_country"
        }
      }
    }
  },
  {
    $unwind: "$sellers"
  },
  {
    $sort: {
      "sellers.avgPrice": 1
    }
  },
  {
    $group: {
      _id: "$_id",
      topSellers: {
        $push: "$sellers"
      }
    }
  },
  {
    $addFields: {
      topSellers: { $slice: ["$topSellers", 3] }
    }
  },
  {
    $project: {
      _id: 0,
      material: "$_id",
      topSellers: 1
    }
  },
  {
    $sort: {
      material: 1
    }
  }
]);



