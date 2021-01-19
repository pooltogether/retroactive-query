BEGIN

CREATE TABLE all_versions_final_deltas AS(
    SELECT * FROM(
    SELECT *,
    "V1" as source
     from `v1`
    UNION ALL
    SELECT *,
        "V2_DAI" as source
         from `v2_dai`
    UNION ALL
    SELECT *,
        "V2_SAI" as source
         from `v2_sai`
    UNION ALL
    SELECT *,
        "V2_USDC" as source
         from `v2_usdc`
    UNION ALL
    SELECT *,
    "V2_USDC_POD" as source
        from `v2_usdc_pods`
    UNION ALL
    SELECT *,
    "V2_DAI_POD" as source
        from `v2_dai_pods`
    UNION ALL    
    SELECT *,
        "V3_UNI" as source
         from `v3_uni`
             UNION ALL 
    SELECT *,
        "V3_DAI" as source
         from `v3_dai`
             UNION ALL 
    SELECT *,
        "V3_USDC" as source
         from `v3_usdc`
    )
    WHERE address NOT IN  ( -- naughty list as of Tuesday 12:58pm  Jan 19th
        "0xa38445311ccd04a54183cdd347e793f4d548df3f",
        "0xe2dc9d379e7f2fd8cfb6872dc4652ae7f073817b",
        "0xa57d294c3a11fb542d524062ae4c5100e0e373ec",
        "0xc5fbf8d4a8b1b5a6acb81808af8ce564309b6df6",
        "0x7808d8cec54602f1b69a12a2804552edfadd1213",
        "0x6a46a33e504d31eb14aba5d573a976a3a3e36e71",
        "0x0e973a46c743f7b3763f07b986138befd2a8a7b9",
        "0xeab60b39012c15db21d7b119d51830fa6ed469f0",
        "0x7efe4864b2674afdb8ee2c8d5b42da3e2e049cff",
        "0x2762f21a6d2cf00e2441d512c89fe269fcfc70bb",
        "0x82b097f6006d7363db8c6e2461ae23aacf879823",
        "0x1f643f0aa8730f53577debae8bbafaf18fd09aeb",
        "0x98ea2d8438f70ce876c2db26fc494cfed10b4cd7",
        "0xfe7205df55ba42c8801e44b55bf05f06cce8565e",
        "0xe0f4217390221af47855e094f6e112d43c8698fe",
        "0x38e842cfc75951d08e9e13bf6a8def90c639c136",
        "0xeeddf4937e3a7abe03e08963c3c20affbd770b51",
        "0xce53382f96fde0db592574ed2571b3307db859ce",
        "0x381843c8b4a4a0da3c0800708c84aa2d792d22b1",
        "0x1613baea8fc7d1a9ca4f8769334f4743be9e362f",
        "0x848acdf334e4bd3c934e1ba9429101c87531bcd1",
        "0xbac372d515b31f6b1bb96692fa4607c642bde601",
        "0xdc558806ad57e080b0b8d26223de6fed576c719f",
        "0xa31152499aae4ad8b6ff344bf429687e38d8dc79",
        "0x11ba3d40f7549485d5b821217e3f4474ae90fecd",
        "0xa847db8fcea81f5652166de4c073e698de884b40",
        "0x573bd3868b7672332c4d22076f55cb0b597eb5fd",
        "0x7cfc5a12506d92f29d52ec7b8d1148f46e9296ed",
        "0x50d6d6195b102f9b58a29a57e3d71822881033a5",
        "0xf6529bb2e96424eb03cb62340980bd760fe9a710",
        "0x2774d92f5674565b84eb8da01ff49a28cc93eac2",
        "0x7576e5315467e711d1c67d547ec5fe936beb7d25",
        "0xe66a59e816c664d2461a71c3bef48d642af8594f",
        "0xae8887db47b1a4ff0f88c481895dce7cff68ae25",
        "0xa145f5436741d9675f84a357249c4b90fae4b77e",
        "0x14300cd613d6a719ac19de68c2f1042b3217cbff",
        "0x567fb0238d73fcf4db40c3bd83433b9a39284cae",
        "0x7c6df34e69f0d84758608a4067ee3d2ab83bfc55",
        "0x5858e98e7a8bec14a099aeaa8c9473803e530a81",
        "0x2409b5d2ec8c31fd9e782d1d2b86680e5a7dafdb",
        "0xcb12fcad55df85f63aeb17eb4de555a5657d4fc2",
        "0xb597ed741869c20ebff5328c69cf3e8f1e903b54",
        "0x3868ada152d8888c5a7154fa47f1ce028a741bde",
        "0xbc981682b955bb0ff7f19c2b3c9344b8c9cb2f6d",
        "0xebfb47a7ad0fd6e57323c8a42b2e5a6a4f68fc1a",
        "0x178969a87a78597d303c47198c66f68e8be67dc2",
        "0x0650d780292142835f6ac58dd8e2a336e87b4393",
        "0xe8726b85236a489a8e84c56c95790d07a368f913",
        "0xde9ec95d7708b8319ccca4b8bc92c0a3b70bf416",
        "0x3d9946190907ada8b70381b25c71eb9adf5f9b7b",
        "0x4d695c615a7aacf2d7b9c481b66045bb2457dfde",
        "0xfe7205df55ba42c8801e44b55bf05f06cce8565e",
        "0x801b4872a635dccc7e679eeaf04bef08e562972a",
        "0x8a4416453340ecf6c489eff3030edb632b0087b2",
        "0x4027de966127af5f015ea1cfd6293a3583892668",
        "0xdb8e47befe4646fcc62be61eee5df350404c124f",
        "0xb7896fce748396ecfc240f5a0d3cc92ca42d7d84",
        "0xfe6892654cbb05eb73d28dcc1ff938f59666fe9f",
        "0x29fe7d60ddf151e5b52e5fab4f1325da6b2bd958",
        "0x49d716dfe60b37379010a75329ae09428f17118d",
        "0x0034ea9808e620a0ef79261c51af20614b742b24",
        "0xbd87447f48ad729c5c4b8bcb503e1395f62e8b98",
        "0x9f4c5d8d9be360df36e67f52ae55c1b137b4d0c4",
        "0x6f5587e191c8b222f634c78111f97c4851663ba4",
        "0xa5c3a513645a9a00cb561fed40438e9dfe0d6a69",
        "0x0a09cd09b0107bb98a83f211704f036eca94b92e",
        "0x58bbb8d3c0c16b35c6d09a8306dd012b61911699",
        "0x14784c77300cdad40b0fded2e1298a62f99b4c21",
        "0xc4ac38dc5d4f58170d9a7183f7c368cbc97264db",
        "0xe507f2d7de97c783a60fef9f1c4a4dade2b0a989",
        "0x2a3f8ed783ff94fc9b4d87c6b7c6b770bbf063d3",
        "0xec0286a4b478ecd600d3d96e398157b4825c5a38",
        "0x555ab21f310d73459d76682e119693a3715d97bc",
        "0x5ff0f990137ed250c84c492a896cb3f980d0f6b9",
        "0xf5276a7166cfda0d68b257e27c7c8bb2e5852e91",
        "0x8f7f92e0660dd92eca1fad5f285c4dca556e433e",
        "0x5e6cc2397ecb33e6041c15360e17c777555a5e63",
        "0x0f1736f70afea9b3863e0894331986845e081868",
        "0x0fd18cca28c3e2c1b0e2cdb12e5387a8e629c048",
        "0xb8d8c8bf9d9c97ab197f9f6e466233d1e7fdccf2",
        "0x975bbe1b1dda507b270b5e482018d89967026f1d",
        "0x690fda326337be9d372beac10ed4c464e097e1ea",
        "0xf35ecbd16e83254b571dda91ca6440bfac06318e",
        "0xc7cbb97f76046ec031202f2da19f9b16b7f18d4a",
        "0x7a3d9330ca9b0d0a1e9d0cc223ce786ce7fa4813",
        "0xaa1fdb5d1fd7d28d216fdfeb3475ccac538c61fa",
        "0x6ff7639052b97965875f51aded19bce0eadb1214",
        "0x5881541e79f5d9d5d72c87e8ea6f681cab289b12"
    )
);

END;