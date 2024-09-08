//Skripta Tijana
//indeksi
db.products.createIndex({ price_usd: 1 });

db.products.createIndex({
    gender_target: 1,
    name: 1,
    price_usd: 1
});

db.products.createIndex({ "category": 1, "type": 1, "seller.num_followers": 1 });

db.products.createIndex({ "season": 1, "state.sold": 1, "category": 1 });


//upit 1
db.products.aggregate([
    {
        $match: {
            "gender_target": "Women",                                      // zenski proizvodi
            "name": { $regex: /jeans/i },                                  // proizvodi koji sadrze rec "jeans" u imenu, naziv nije case-sensitive
            "price_usd": { $lte: 200 },                                    // cena manja ili jednaka 200
            "price_usd": { $exists: true, $ne: null }                      // postoji cena i nije null
        }
    },
    {
        $addFields: {
            "price_usd_numeric": { $toDouble: "$price_usd" }               // dodavanje polja za numericku vrednost cene
        }
    },
    {
        $group: {
            _id: {
                season: "$season",                                         // grupisanje po obelezju "season"
                material: "$material"                                      // grupisanje po obelezju "material"
            }, 
            averagePrice: { $avg: "$price_usd_numeric" },                  // racunanje prosecne cene
            minPrice: { $min: "$price_usd_numeric" },                      // racunanje minimalne cene
            maxPrice: { $max: "$price_usd_numeric" },                      // racunanje maksimalne cene
            colors: { $push: "$color" },                                   // cuvanje svih boja iz grupe
            count: { $sum: 1 }                                             // brojanje proizvoda u grupi
        }
    },
    {
        $addFields: {
            mostCommonColor: {
                $arrayElemAt: [
                    {
                        $map: {
                            input: { $range: [0, { $size: "$colors" }] },
                            in: {
                                color: { $arrayElemAt: ["$colors", "$$this"] },
                                count: {
                                    $size: {
                                        $filter: {
                                            input: "$colors",
                                            as: "color",
                                            cond: { $eq: ["$$color", { $arrayElemAt: ["$colors", "$$this"] }] }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    { $indexOfArray: ["$colors", { $max: "$colors.count" }] }
                ]
            },
        }
    },
    {
        $sort: { "averagePrice": -1 }                                         // sortiranje po prosecnoj ceni, opadajuce
    },
    {
        $limit: 30                                                            // ogranicenje na prvih 10 rezultata
    },
    {
        $project: {
            _id: 0,
            "season_material": { $concat: ["$_id.season", " - ", "$_id.material"] },            // spajanje obelezja "season" i "material"
            mostCommonColor: "$mostCommonColor.color",
            averagePrice: { $round: ["$averagePrice", 2] },                                     // zaokruživanje cena na dve decimale
            minPrice: { $round: ["$minPrice", 2] },  
            maxPrice: { $round: ["$maxPrice", 2] }  
        }
    }
]).explain("executionStats");

//upit 2
db.products.aggregate([
    {
        $addFields: {
            "seller.num_followers_double": { $toDouble: "$seller.num_followers" }                   //konverzija broja pratilaca u broj
        }
    },
    {
        $group: {                                                                                   //grupisanje po tipu i kategoriji
            _id: {
                category: "$category",
                type: "$type"
            },
            totalFollowers: { $sum: "$seller.num_followers_double" }                                 //ukupan broj pratilaca za svaku kombinaciju kategorije i tipa
        }
    },
    {
        $group: {
            _id: "$_id.category",
            totalFollowers: { $sum: "$totalFollowers" },                                             //ukupan broj pratilaca za svaku kategoriju
            types: {                                                                                 //grupise sve tipove proizvoda unutar svake kategorije
                $push: {                                                                             //i cuva njihove ukupne brojeve pratilaca
                    type: "$_id.type",
                    totalFollowers: "$totalFollowers"
                }
            }
        }
    },
    {
        $addFields: {                                                                                //najpopularniji tip unutar svake kategorije
            mostPopularType: {
                $arrayElemAt: [                                                                      //izvlaci prvi element iz filtrirane liste
                    {
                        $filter: {                                                                   //filtrira tipove da bi pronasao onaj sa najvecim brojem pratilaca
                            input: "$types",
                            as: "type",
                            cond: {
                                $eq: ["$$type.totalFollowers", { $max: "$types.totalFollowers" }]
                            }
                        }
                    },
                    0                                                                                  //indeks prvog elementa u rezultujucem nizu
                ]
            }
        }
    },
    {
        $project: {                                                                                     //sortirani ispis
            _id: 0,
            category: "$_id",
            totalFollowers: 1,
            mostPopularType: "$mostPopularType.type"
        }
    },
    {
        $sort: { totalFollowers: -1 }
    }
])


//upit 3
db.products.aggregate([
    {
        $group: {
            _id: "$season",
            totalSold: {
                $sum: { $cond: [{ $eq: ["$state.sold", "True"] }, 1, 0] }                     //racunanje ukupnog broja prodatih proizvoda po sezoni
            },
            totalNotSold: {
                $sum: { $cond: [{ $eq: ["$state.sold", "False"] }, 1, 0] }                    //racunanje ukupnog broja neprodatih proizvoda po sezoni
            },
            leastSoldCategories: {
                $push: "$category"                                                            //cuvanje svih kategorija iz neprodatih proizvoda
            }
        }
    },
    {
        $addFields: {
            totalProducts: { $sum: ["$totalSold", "$totalNotSold"] },                       // ukupan broj proizvoda po sezoni
            totalSold: { $sum: "$totalSold" },                                              // ukupan broj prodatih proizvoda po sezoni
            totalNotSold: { $sum: "$totalNotSold" }                                         // ukupan broj neprodatih proizvoda po sezoni
        }
    },
    {
        $addFields: {
            leastSoldCategory: {
                $arrayElemAt: [
                    {
                        $map: {
                            input: { $setUnion: ["$leastSoldCategories"] },                 // uklanjanje duplikata kategorija
                            as: "category",
                            in: {
                                category: "$$category",
                                count: {
                                    $size: {
                                        $filter: {
                                            input: "$leastSoldCategories",
                                            as: "cat",
                                            cond: { $eq: ["$$cat", "$$category"] }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    { $indexOfArray: ["$leastSoldCategories", { $max: "$leastSoldCategories.count" }] }
                ]
            }
        }
    },
    {
        $lookup: {                                                                                                    //dodavanje dodatnih detalja o najmanje prodatoj kategoriji
            from: "products",
            let: { season: "$_id", category: "$leastSoldCategory.category" },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$season", "$$season"] },
                                { $eq: ["$category", "$$category"] },
                                { $eq: ["$state.sold", "False"] }
                            ]
                        }
                    }
                },
                {
                    $group: {
                        _id: "$category",
                        count: { $sum: 1 }
                    }
                },
                {
                    $sort: { count: -1 }                             // sortiranje po broju pojavljivanja neprodatih proizvoda
                },
                {
                    $limit: 1
                }
            ],
            as: "leastSoldCategoryDetails"
        }
    },
    {
        $addFields: {
            leastSoldCategory: {
                category: { $arrayElemAt: ["$leastSoldCategoryDetails._id", 0] },                  // uzimanje naziva kategorije
                count: { $arrayElemAt: ["$leastSoldCategoryDetails.count", 0] }                    // broj proizvoda te kategorije koji nisu prodani
            }
        }
    },
    {
        $addFields: {
            percentageSold: {
                $cond: {
                    if: { $gt: ["$totalProducts", 0] },
                    then: { $multiply: [{ $divide: ["$totalSold", "$totalProducts"] }, 100] },
                    else: 0                                                                          // ako ne postoji nijedan proizvod, procenat je 0
                }
            }
        }
    },
    {
        $sort: { "percentageSold": -1 }                                                             // sortiranje po procentu prodaje opadajuce
    },
    {
        $project: {
            _id: 0,
            season: "$_id",
            totalSold: 1,
            totalNotSold: 1,
            totalProducts: 1,
            percentageSold: { $round: ["$percentageSold", 2] },                                                  // zaokruzenje na dve decimale
            percentageSoldString: { $concat: [{ $toString: { $round: ["$percentageSold", 2] } }, "%"] },         // dodavanje postotnog znaka
            leastSoldCategory: "$leastSoldCategory.category"                                                     // prikazivanje samo naziva kategorije
        }
    }
]);


//upit 4
db.products.aggregate([
    {
        $addFields: {
            like_count_double: { $toDouble: "$like_count" },
            price_usd_double: { $toDouble: "$price_usd" }
        }
    },
    {
        $group: {
            _id: null,
            averageLikes: { $avg: "$like_count_double" }                                //prosecan broj lajkova svih proizvoda
        }
    },
    {
        $lookup: {                                                                     //lookup operacija za filtriranje proizvoda sa lajkovima vecim od proseka
            from: "products",
            let: { averageLikes: "$averageLikes" },
            pipeline: [
                {
                    $addFields: {
                        like_count_double: { $toDouble: "$like_count" }
                    }
                },
                {
                    $match: {                                                              // filtriranje proizvoda koji imaju broj lajkova veci od prosecnog broja lajkova
                        $expr: { $gt: ["$like_count_double", "$$averageLikes"] }
                    }
                },
                {
                    $unwind: "$keywords"                                                    //razdvajanje polja "keywords" na pojedinacne vrednosti
                },
                {
                    $group: {
                        _id: {                                                                 //grupisanje prema "condition" i "keywords",
                            condition: "$condition",                                           //racunanje pojavljivanja i cuvanje materijala
                            keyword: "$keywords"
                        },
                        count: { $sum: 1 },
                        materials: { $push: "$material" }
                    }
                },
                {
                    $sort: { count: -1 }
                },
                {
                    $group: {                                                                   //grupisanje po "condition" i uzimanje najcesce kljucne reci i materijala
                        _id: "$_id.condition",
                        topKeyword: { $first: "$_id.keyword" },
                        topMaterial: { $first: { $arrayElemAt: ["$materials", 0] } },
                        count: { $first: "$count" }
                    }
                }
            ],
            as: "conditionKeywords"
        }
    },
    {
        $unwind: "$conditionKeywords"
    },
    {
        $project: {
            condition: "$conditionKeywords._id",
            topKeyword: "$conditionKeywords.topKeyword",
            topMaterial: "$conditionKeywords.topMaterial",
            count: "$conditionKeywords.count",
            _id: 0
        }
    }
]);


//upit 5
db.products.aggregate([
    {
        $facet: {                                                             //prikuplja podatke o najjeftinijim i najskupljim crnim haljinama i cipelama za zene
            cheapestBlackDress: [
                {
                    $match: {
                        "gender_target": "Women",
                        "name": { $regex: /dress/i },
                        "color": "Black",
                        "price_usd": { $exists: true, $ne: null }
                    }
                },
                {
                    $addFields: {
                        "price_usd_numeric": { $toDouble: "$price_usd" }
                    }
                },
                {
                    $sort: { "price_usd_numeric": 1 }
                },
                {
                    $limit: 1
                },
                {
                    $project: {
                        _id: 0,
                        type: "Dress",
                        color: "Black",
                        productName: "$name",
                        productPrice: { $round: ["$price_usd_numeric", 2] },
                        material: 1
                    }
                }
            ],
            cheapestBlackShoes: [
                {
                    $match: {
                        "gender_target": "Women",
                        "name": { $regex: /shoes/i },
                        "color": "Black",
                        "price_usd": { $exists: true, $ne: null }
                    }
                },
                {
                    $addFields: {
                        "price_usd_numeric": { $toDouble: "$price_usd" }
                    }
                },
                {
                    $sort: { "price_usd_numeric": 1 }
                },
                {
                    $limit: 1
                },
                {
                    $project: {
                        _id: 0,
                        type: "Shoes",
                        color: "Black",
                        productName: "$name",
                        productPrice: { $round: ["$price_usd_numeric", 2] }
                    }
                }
            ],
            mostExpensiveBlackDress: [
                {
                    $match: {
                        "gender_target": "Women",
                        "name": { $regex: /dress/i },
                        "color": "Black",
                        "price_usd": { $exists: true, $ne: null }
                    }
                },
                {
                    $addFields: {
                        "price_usd_numeric": { $toDouble: "$price_usd" }
                    }
                },
                {
                    $sort: { "price_usd_numeric": -1 }
                },
                {
                    $limit: 1
                },
                {
                    $project: {
                        _id: 0,
                        type: "Dress",
                        color: "Black",
                        productName: "$name",
                        productPrice: { $round: ["$price_usd_numeric", 2] },
                        material: 1
                    }
                }
            ],
            mostExpensiveBlackShoes: [
                {
                    $match: {
                        "gender_target": "Women",
                        "name": { $regex: /shoes/i },
                        "color": "Black",
                        "price_usd": { $exists: true, $ne: null }
                    }
                },
                {
                    $addFields: {
                        "price_usd_numeric": { $toDouble: "$price_usd" }
                    }
                },
                {
                    $sort: { "price_usd_numeric": -1 }
                },
                {
                    $limit: 1
                },
                {
                    $project: {
                        _id: 0,
                        type: "Shoes",
                        color: "Black",
                        productName: "$name",
                        productPrice: { $round: ["$price_usd_numeric", 2] }
                    }
                }
            ]
        }
    },
    {
        $project: {
            cheapestBlackDress: { $arrayElemAt: ["$cheapestBlackDress", 0] },                         //prikazivanje samo prvog elementa iz svake fasete
            cheapestBlackShoes: { $arrayElemAt: ["$cheapestBlackShoes", 0] },
            mostExpensiveBlackDress: { $arrayElemAt: ["$mostExpensiveBlackDress", 0] },
            mostExpensiveBlackShoes: { $arrayElemAt: ["$mostExpensiveBlackShoes", 0] }
        }
    },
    {
        $addFields: {
            cheapestTotal: {                                                                            //dodavanje polja za ukupnu cenu najjeftinijih i najskupljih kombinacija
                $sum: ["$cheapestBlackDress.productPrice", "$cheapestBlackShoes.productPrice"]
            },
            mostExpensiveTotal: {
                $sum: ["$mostExpensiveBlackDress.productPrice", "$mostExpensiveBlackShoes.productPrice"]
            }
        }
    },
    {
        $lookup: {                                                                                     //lookup za pronalazenje proizvoda koji su skuplji od najjeftinijih, 
                                                                                                       //ali jeftiniji od najskupljih crnih proizvoda od istog materijala
            from: "products",
            let: {
                minPrice: "$cheapestTotal",
                maxPrice: "$mostExpensiveTotal",
                material: "$mostExpensiveBlackDress.material"
            },
            pipeline: [
                {
                    $addFields: {
                        price_usd_numeric: { $toDouble: "$price_usd" }
                    }
                },
                {
                    $match: {                                                                         //filtriranje proizvoda prema cenovnom rangu, boji i materijalu
                        $expr: {
                            $and: [
                                { $gt: ["$price_usd_numeric", "$$minPrice"] },
                                { $lt: ["$price_usd_numeric", "$$maxPrice"] },
                                { $eq: ["$color", "Black"] },
                                { $eq: ["$material", "$$material"] }
                            ]
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        productName: "$name",
                        productPrice: { $round: ["$price_usd_numeric", 2] },
                        productColor: "$color",
                        productGender: "$gender_target",
                        productMaterial: "$material"
                    }
                },
                {
                    $sort: { "price_usd_numeric": 1 }
                },
                {
                    $limit: 10
                }
            ],
            as: "productsInRange"
        }
    },
    {
        $project: {
            cheapestBlackDress: 1,
            cheapestBlackShoes: 1,
            mostExpensiveBlackDress: 1,
            mostExpensiveBlackShoes: 1,
            cheapestTotal: 1,
            mostExpensiveTotal: 1,
            productsInRange: 1
        }
    }
]);





