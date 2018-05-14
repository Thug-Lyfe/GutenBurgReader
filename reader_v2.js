const fs = require('graceful-fs');
const cities = require("all-the-cities");
const stringify = require('csv-stringify');
const { fork } = require('child_process');
const pool = require('fork-pool');
var async = require('async');

console.time("dbsave");
let totalTime = Date.now();

let psql_auth = []

fs.writeFile('csvs/mongo_auth.json', '', function () { console.log('done') })
fs.writeFile('csvs/mongo_book.json', '', function () { console.log('done') })
fs.writeFile('csvs/mongo_city.json', '', function () { console.log('done') })
fs.writeFile('csvs/neo4j_auth_book.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_author.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_book.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_city.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_mention.csv', '', function () { console.log('done') })


let city_ids = []


let cp = new pool('./cityscan.js', null, null, {});
let successFunc_meta = async function (id, filename, auth, title, release, callback) {
    let auth_id = -1;
    let book_id = count_book++;
    for (var i = 0; i < psql_auth.length; i++) {
        if (psql_auth[i][1] == auth) {
            auth_id = i;
        }
    }
    if (auth_id == -1) {
        auth_id = psql_auth.length
        psql_auth.push([auth_id, auth])
        appendToCsv([auth_id, "\"" + auth + "\""], "psql_author.csv")
        appendToJson({
            _id: "auth" + auth_id,
            name: auth
        }, "mongo_auth.json")
    }
    appendToCsv([book_id, "\"" + filename + "\"", auth_id, "\"" + title + "\"", release], "psql_book.csv")
    appendToCsv([auth_id, book_id], "neo4j_auth_book.csv");
    callback(book_id, {
        _id: "book" + book_id,
        filename: filename,
        author: auth,
        title: title,
        release_date: release,
        cities: []
    })
}

let failFunc = function (filename, str, id, dirname) {
    fail_count++;
    console.log(str, "filename: " + filename, "id: " + id, "dirname: " + dirname, "  ::  ", filename.substring(0, filename.indexOf(".")).match(/^[0-9]*$/))
}

let cpQ = async function (content, book_id, mongo_temp, callback) {
    cp.enqueue(content, (err, list) => {
        list.stdout.city_list.forEach(function (city_id) {
            if (!city_ids.includes(city_id)) {
                city_ids.push(city_id)
                appendToCsv([city_id, "\"" + cities[city_id].name + "\"", cities[city_id].lat, cities[city_id].lon], "psql_city.csv")
                appendToJson({
                    _id: "city" + city_id,
                    name: cities[city_id].name,
                    location: { type: 'Point', coordinates: [cities[city_id].lon, cities[city_id].lat] }
                }, "mongo_city.json")
            }
            mongo_temp.cities.push("city" + city_id)
            appendToCsv([book_id, city_id], "psql_mention.csv")
        })
        appendToJson(mongo_temp, "mongo_book.json")

        callback("books fin: " + ++count_fin + "\tbooks remaining: " + (count - count_fin) + "\t% done: " + Math.floor(count_fin * 10000 / count) / 100 + "\ttotal time: " + (Date.now() - totalTime) / 1000 + "s", )
    })

}
let appendToCsv = function (content, file) {
    stringify(content, function (err, output) {
        fs.appendFile('csvs/' + file, content + '\n', 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                //console.log('psql_book.csv is saved!');
            }
        });
    });
}

let appendToJson = function (content, file) {
    fs.appendFile('csvs/' + file, JSON.stringify(content), 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else {
            //console.log('mongo_auth.json is saved!');
        }
    });
}

let somefunc = async function (filename, dirname, content, callback) {
    let title = null; let auth = null; let release = null;
    let index = content.indexOf("[Etext #");
    let id = null;
    if (filename.substring(0, filename.indexOf(".")).match(/^[0-9]*$/) != null) {
        id = filename.substring(0, filename.indexOf("."));
    }
    else {
        if ((index == -1 || content.indexOf("[EBook #") < index) && content.indexOf("[EBook #") != -1) {
            index = content.indexOf("[EBook #");
        }
        if (index != -1) {
            id = content.substring(index + 8, content.indexOf("]", index));
        }
    }

    if (filename.indexOf("G-") == 0) {
        title_ind = content.indexOf("\n") + 1
        title = content.substring(title_ind, content.indexOf("1", title_ind)).replace(/(\r\n\t|\n|\r\t|\*|\r)/gm, "");
        auth = "unknown";
        release = "unknown";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length), auth, title, release, (book_id, mongo_temp) => {
            cpQ(content, book_id, mongo_temp, (res) => {
                callback(res)
            })
        });

    }
    else if (filename == "baleng2.txt") {
        title = "Ancient Poems, Ballads and Songs of the Peasantry of England";
        auth = "Robert Bell";
        release = "1846";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length), auth, title, release, (book_id, mongo_temp) => {
            cpQ(content, book_id, mongo_temp, (res) => {
                callback(res)
            })
        });
    }
    else if (filename == "pntvw10.txt") {
        title = "The Point of View";
        auth = "Henry James";
        release = "01-10-2001";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length), auth, title, release, (book_id, mongo_temp) => {
            cpQ(content, book_id, mongo_temp, (res) => {
                callback(res)
            })
        });
    }
    else if (filename == "Introduction_and_Copyright.txt") {
        title = "The Common New Testament";
        auth = "Timothy Clontz";
        release = "14-03-1999";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length), auth, title, release, (book_id, mongo_temp) => {
            cpQ(content, book_id, mongo_temp, (res) => {
                callback(res)
            })
        });
    }
    else {
        fs.readFile("cache/epub/" + id + "/pg" + id + ".rdf", 'utf-8', function (err, meta_data) {

            if (meta_data != undefined && meta_data != null) {
                if (meta_data.indexOf("<dcterms:title>") != -1) {
                    title = meta_data.substring(meta_data.indexOf("<dcterms:title>") + 15, meta_data.indexOf("</dcterms:title>")).replace(/(\r\n\t|\n|\r\t|\*|\r)/gm, " ");
                } else {
                    title = "unknown";
                }
                if (meta_data.indexOf("<pgterms:name>") != -1) {
                    auth = meta_data.substring(meta_data.indexOf("<pgterms:name>") + 14, meta_data.indexOf("</pgterms:name>")).replace(/(\r\n\t|\n|\r\t|\*|\r)/gm, " ");
                } else {
                    auth = "unknown";
                }
                if (meta_data.indexOf("</dcterms:issued>") != -1) {
                    release = meta_data.substring(meta_data.indexOf("</dcterms:issued>") - 10, meta_data.indexOf("</dcterms:issued>")).replace(/(\r\n\t|\n|\r\t|\*|\r)/gm, " ");
                } else {
                    release = "unknown";
                }
            } else {
                failFunc(filename, "meta_data err: ", id, dirname);
            }
            if (title == "unknown") {
                failFunc(filename, "unknown title: ", id, dirname);

            } else {
                successFunc_meta(id, dirname.substring(6, dirname.length), auth, title, release, (book_id, mongo_temp) => {
                    cpQ(content, book_id, mongo_temp, (res) => {
                        callback(res)
                    })
                });
            }
        })
    }
}


let someErr = function (err, index) {
    console.log(index, err)
}

function readFiles_v2(dirname) {
    if (count_dir < tmp_dirs.length) {
        let filename = tmp_dirs[count_dir++]
        if (filename.indexOf('.') == -1) {
            let file_count = 0;
            filenames = fs.readdirSync(dirname + filename + "/");
            filenames.forEach(function (file) {
                if (file.indexOf('.txt') != -1) {
                    fs.readFile(dirname + filename + "/" + file, 'utf-8', function (err, content) {
                        if (err) {
                            onError(err);
                            return;
                        }
                        somefunc(file, dirname + filename , content, (res) => {
                            console.log(res, "\tavg time: " + Math.round((Date.now() - totalTime) / count_fin) + "ms", "\tdir: " + dirname + filename + "/" + file)
                            if (++file_count == filenames.length) {
                                readFiles_v2(dirname)
                            }
                        })
                    })
                }
            })
        }
        else if (filename.indexOf('.txt') != -1) {
            fs.readFile(dirname + filename, 'utf-8', function (err, content) {
                if (err) {
                    onError(err);
                    return;
                }
                somefunc(filename, dirname + filename, content, (res) => {
                    console.log(res, "\tavg time: " + Math.round((Date.now() - totalTime) / count_fin) + "ms", "\tdir: " + dirname + filename)
                    readFiles_v2(dirname)
                })
            })
        }
    }else{
        if(--inst_count == 0){
            console.log("done")
        }
    }
}



let scanCities_v2 = function (content, callback) {

    let index = content.indexOf("*** START OF THIS PROJECT GUTENBERG") + 35;
    let end = content.indexOf("*** END OF THIS PROJECT GUTENBERG");
    if (end == -1) {
        end = content.length
    }
    content = content.substring(index, end)
    let reg = new RegExp(/\b^[A-Z].*?\b/, 'gm')
    let found = content.match(reg)
    let list = [];
    cities.forEach(function (city, index) {
        if (city.name.match(/[^\w\*]/, 'gm') == null) {
            if (found.includes(city.name)) {
                list.push(index);
            }
        } else {
            if (content.indexOf(" " + city.name + " ") != -1) {

                list.push(index);
            }
        }
    })
    callback(list)
}

let count = 37237;
let count_fin = 0;
let count_book = 0;
let count_dir = 0;
let target = 'files/';
let tmp_dirs = fs.readdirSync(target);
let inst_count = 100;
for (i = 0; i < inst_count; i++) {
    readFiles_v2(target)
}
