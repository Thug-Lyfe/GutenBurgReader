mongoimport --db booktest --collection auths --file mongo_auth.json
mongoimport --db booktest --collection books --file mongo_book.json --jsonArray
node --max_old_space_size=2000000 .\reader_v2.js