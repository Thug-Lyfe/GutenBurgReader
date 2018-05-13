//const fs = require('fs');
const fs = require('graceful-fs');
const cities = require("all-the-cities");
const stringify = require('csv-stringify');
const { fork } = require('child_process');
const pool = require('fork-pool');
var async = require('async');
//const mongoID = require('mongodb').ObjectID;

//let suc_title = 0; let suc_auth = 0; let suc_release = 0; let count = 0;
//let fail_count = 0;
//let list_ids = [];
console.time("dbsave");
let totalTime = Date.now();
//list of psql
let psql_auth = []
//let psql_book = []
//let psql_mens = []
//let psql_city = []
//let auth_book = []

//let mongo_auth = []
//let mongo_book = []
//let mongo_city = [];





fs.writeFile('csvs/mongo_auth.json', '', function () { console.log('done') })
fs.writeFile('csvs/mongo_book.json', '', function () { console.log('done') })
fs.writeFile('csvs/mongo_city.json', '', function () { console.log('done') })
fs.writeFile('csvs/neo4j_auth_book.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_author.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_book.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_city.csv', '', function () { console.log('done') })
fs.writeFile('csvs/psql_mention.csv', '', function () { console.log('done') })


let city_ids = []

let count = 37234;
let count_fin = 0;
let count_book = 0;
let cp = new pool('./cityscan.js', null, null, {});
let successFunc_meta = async function (id, filename, auth, title, release, callback) {
    let auth_id = -1;
    let book_id = count_book++;
    for (var i = 0; i < psql_auth.length; i++) {
        if (psql_auth[i][1] == auth) {
            auth_id = i;
            //mongo_auth[i].written.push("book" + book_id);
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
    let localTime = Date.now();
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
            //console.log(mongo_temp)
            mongo_temp.cities.push("city" + city_id)
            appendToCsv([book_id, city_id], "psql_mention.csv")
        })
        appendToJson(mongo_temp, "mongo_book.json")

        //console.log("books fin: " + ++count_fin, "\tbooks remaining: " + (count - count_fin), "\t% done: " + Math.floor(count_fin * 10000 / count) / 100, "\ttotal time: " + (Date.now() - totalTime) / 1000 + "s", )

        if (count_fin == count) {
            console.timeEnd("dbsave")
            //writeToCsv();
            /*cp.drain((test) => {
            });*/
        }
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
/*
let writeToCsv = function () {
    stringify(psql_auth, function (err, output) {
        fs.writeFile('csvs/psql_author.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('psql_author.csv is saved!');
            }
        });
    });
}*/
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
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id, mongo_temp) => {
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
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id, mongo_temp) => {
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
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id, mongo_temp) => {
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
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id, mongo_temp) => {
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
                successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id, mongo_temp) => {
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

function readFiles(dirname) {
    console.log(dirname + tmp_dirs[dir_count])
    filenames = fs.readdirSync(dirname + tmp_dirs[dir_count] + "/");
    filenames.forEach(function (file) {
        if (file.indexOf(''))
            fs.readFile(dirname + tmp_dirs[dir_count] + "/" + file, 'utf-8', function (err, content) {

                if (err) {
                    onError(err);
                    return;
                }
                somefunc(file, dirname + tmp_dirs[dir_count] + "/", content, (res) => {

                    console.log(res, "\tdir: " + dirname + tmp_dirs[dir_count] + "/" + file)
                    if (++file_count == filenames.length) {
                        dir_count++;
                        file_count = 0;
                        readFiles(dirname)
                    }
                })
            })
    })
}

function readFiles_v2(dirname) {

    let filename = tmp_dirs[count_dir++]
    console.log(dirname + filename)
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
                    somefunc(file, dirname + filename + "/", content, (res) => {
                        console.log(res, "\tdir: " + dirname + filename + "/" + file)
                        if (++file_count == filenames.length) {
                            readFiles_v2(dirname)
                        }
                    })
                })
            }
        })
    }
    else if (filename.indeOf('.txt') != -1) {
        fs.readFile(dirname + filename, 'utf-8', function (err, content) {
            if (err) {
                onError(err);
                return;
            }
            somefunc(filename, dirname + filename + "/", content, (res) => {
                console.log(res, "\tdir: " + filename + "/" + filename)
                    readFiles_v2(dirname)
            })
        })
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

//use test/ for test files, or files/ for all files
//remember test if at line 87.
//readFiles("files/", somefunc, 

/*
let redirect = function (target_dir, from_dir) {
    fs.readdir(from_dir, function (err, filenames) {
        if (err) {
            throw err;
            return;
        }
        filenames.forEach(function (filename) {
            if (filename.indexOf(".") == -1) {
                redirect(target_dir, from_dir + filename + "/");
            }
            if (filename.indexOf(".txt") != -1) {
                if (count_files == 0) {
                    if (!fs.existsSync(target_dir + count_dir)) {
                        fs.mkdirSync(target_dir + count_dir)
                    }
                }
                fs.rename(from_dir + filename, target_dir + count_dir + "/" + filename, (err) => {
                    if (err) throw err;
                    console.log('Rename complete!');
                });

                if (++count_files == 300) {
                    count_files = 0
                    count_dir++;
                }
            }
        });
    })
}*/
let redirect_v2 = function (target_dir) {
    tmp_dirs = fs.readdirSync(target_dir);
    readFiles(target_dir, somefunc)
}
//for renaming
let count_dir = 0;
//let count_files = 0;
//for recursive
let dir_count = 0;
let file_count = 0;
let target = 'files/'
let tmp_dirs = null

redirect_v2(target)
