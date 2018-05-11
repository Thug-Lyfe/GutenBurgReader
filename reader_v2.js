//const fs = require('fs');
const fs = require('graceful-fs');
const cities = require("all-the-cities");
const stringify = require('csv-stringify');
const { fork } = require('child_process');
const pool = require('fork-pool');
//const mongoID = require('mongodb').ObjectID;

//let suc_title = 0; let suc_auth = 0; let suc_release = 0; let count = 0;
//let fail_count = 0;
//let list_ids = [];
console.time("dbsave");
//list of psql
let psql_auth = []
let psql_book = []
let psql_mens = []
let psql_city = []
let auth_book = []

let mongo_auth = []
let mongo_book = []
let mongo_city = [];

let city_ids = []

let count = 0;
let count_fin = 0;
let cp = new pool('./cityscan.js', null, null, {});
let successFunc_meta = function (id, filename, auth, title, release, callback) {
    let auth_id = -1;
    let book_id = psql_book.length
    for (var i = 0; i < psql_auth.length; i++) {
        if (psql_auth[i][1] == auth) {
            auth_id = i;
            mongo_auth[i].written.push("book" + book_id);
        }
    }
    if (auth_id == -1) {
        auth_id = psql_auth.length
        psql_auth.push([auth_id, auth])
        mongo_auth.push({
            _id: "auth"+auth_id,
            name: auth,
            written: ["book"+book_id]
        });
    }

    psql_book.push([book_id, filename, auth_id, title, release])
    auth_book.push([auth_id, book_id])
    mongo_book.push({
        _id: "book" + book_id,
        filename: filename,
        author: auth,
        title: title,
        release_date: release,
        cities: []
    })


    callback(book_id)

    /*count++;
    if (tit != "unknown") {
        suc_title++;
    }
    if (au != "unknown") {
        suc_auth++;
    }
    if (rel != "unknown") {
        suc_release++;
    }*/

    //console.log("fails: " + fail_count, "files: " + count, "title%: " + Math.floor(suc_title * 100000 / count) / 1000 + "%", "auth%: " + Math.floor(suc_auth * 100000 / count) / 1000 + "%", "release%: " + Math.floor(suc_release * 100000 / count) / 1000 + "%")

}

let successFunc_city = function (book_id, list, callback) {
    list.forEach(function (city_id) {
        if (!city_ids.includes(city_id)) {
            city_ids.push(city_id)

            psql_city.push([city_id, cities[city_id].name, cities[city_id].lat, cities[city_id].lon])

            mongo_city.push({
                _id: "city"+city_id,
                name: cities[city_id].name,
                location: { type: 'Point', coordinates: [cities[city_id].lon, cities[city_id].lat] }
            })
        }
        mongo_book[book_id].cities.push("city" + city_id)
        psql_mens.push([book_id, city_id])
    })
    callback(book_id)
}
let failFunc = function (filename, str, id, dirname) {
    fail_count++;
    console.log(str, "filename: " + filename, "id: " + id, "dirname: " + dirname, "  ::  ", filename.substring(0, filename.indexOf(".")).match(/^[0-9]*$/))
}

let cpQ = function (content, book_id) {
    cp.enqueue(content, (err, list) => {
        successFunc_city(book_id, list.stdout.city_list, (test) => {
            
            console.log("books fin: "+ ++count_fin, "   books remaining: "+(count-count_fin),"    % done: "+Math.floor(count_fin*10000/count)/100)
            //for testing purposes only
            if (count_fin == count) {
                console.timeEnd("dbsave")
                writeToCsv();
                cp.drain((test) => {


                });
            }
        })
    })
}

let writeToCsv = function () {
    stringify(psql_book, function (err, output) {
        fs.writeFile('csvs/psql_book.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('psql_book.csv is saved!');
            }
        });
    });
    stringify(psql_auth, function (err, output) {
        fs.writeFile('csvs/psql_author.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('psql_author.csv is saved!');
            }
        });
    });
    stringify(psql_city, function (err, output) {
        fs.writeFile('csvs/psql_city.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('psql_city.csv is saved!');
            }
        });
    });
    stringify(psql_mens, function (err, output) {
        fs.writeFile('csvs/psql_mention.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('psql_mention.csv is saved!');
            }
        });
    });
    stringify(auth_book, function (err, output) {
        fs.writeFile('csvs/neo4j_auth_book.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('neo4j_auth_book.csv is saved!');
            }
        });
    });

    fs.writeFile('csvs/mongo_auth.json', JSON.stringify(mongo_auth), 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else {
            console.log('mongo_auth.json is saved!');
        }
    });

    fs.writeFile('csvs/mongo_book.json', JSON.stringify(mongo_book), 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else {
            console.log('mongo_book.json is saved!');
        }
    });

    fs.writeFile('csvs/mongo_city.json', JSON.stringify(mongo_city), 'utf8', function (err) {
        if (err) {
            console.log('Some error occured - file either not saved or corrupted file saved.');
        } else {
            console.log('mongo_city.json is saved!');
        }
    });
}

let somefunc = function (filename, dirname, content) {
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
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id) => {
            cpQ(content, book_id)
        });

    }
    else if (filename == "baleng2.txt") {
        title = "Ancient Poems, Ballads and Songs of the Peasantry of England";
        auth = "Robert Bell";
        release = "1846";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id) => {
            cpQ(content, book_id)
        });
    }
    else if (filename == "pntvw10.txt") {
        title = "The Point of View";
        auth = "Henry James";
        release = "01-10-2001";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id) => {
            cpQ(content, book_id)
        });
    }
    else if (filename == "Introduction_and_Copyright.txt") {
        title = "The Common New Testament";
        auth = "Timothy Clontz";
        release = "14-03-1999";
        id = filename
        successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id) => {
            cpQ(content, book_id)
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
                successFunc_meta(id, dirname.substring(6, dirname.length) + filename, auth, title, release, (book_id) => {
                    cpQ(content, book_id)
                });
            }
        })
    }
}


let someErr = function (err, index) {
    console.log(index, err)
}

function readFiles(dirname, onFileContent, onError, callback) {
    fs.readdir(dirname, function (err, filenames) {
        if (err) {
            onError(err);
            return;
        }

        filenames.forEach(function (filename) {
            if (filename.indexOf(".") == -1) {
                readFiles(dirname + filename + "/", somefunc, someErr);
            }
            if (filename.indexOf(".txt") != -1) {
                count++;
                fs.readFile(dirname + filename, 'utf-8', function (err, content) {
                    if (err) {
                        onError(err);
                        return;
                    }
                    onFileContent(filename, dirname, content);
                });
            }
        });
    });

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
readFiles("test/", somefunc, someErr)

