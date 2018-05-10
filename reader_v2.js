//const fs = require('fs');
const fs = require('graceful-fs');
const cities = require("all-the-cities");
const stringify = require('csv-stringify');
const { fork } = require('child_process');
const pool = require('fork-pool');
//const parser = require("xml2json");
//let suc_title = 0; let suc_auth = 0; let suc_release = 0; let count = 0;
//let fail_count = 0;
//let list_ids = [];
console.time("dbsave");
//list of psql
let psql_auth = []
let psql_book = []
let psql_mens = []
let city_ids = []
let psql_city = []
let auth_book = []
let threads = []
let count = 0;
let cp = new pool('./cityscan.js',null,null,{});
let successFunc_meta = function (id, filename, auth, title, release, callback) {


    let auth_id = -1;
    for (var i = 0; i < psql_auth.length; i++) {
        if (psql_auth[i].name == auth) {
            auth_id = i;
        }
    }
    if (auth_id == -1) {
        auth_id = psql_auth.length
        psql_auth.push([auth_id, auth])

    }

    let book_id = psql_book.length
    psql_book.push([book_id, filename, auth_id, title, release])
    auth_book.push([auth_id, book_id])
    //console.log(psql_book)
    //console.log(psql_auth)

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
        }
        psql_mens.push([book_id, city_id])
    })
    callback(book_id)
}
let failFunc = function (filename, str, id, dirname) {
    fail_count++;
    console.log(str, "filename: " + filename, "id: " + id, "dirname: " + dirname, "  ::  ", filename.substring(0, filename.indexOf(".")).match(/^[0-9]*$/))
}

let writeToCsv = function () {
    stringify(psql_book, function (err, output) {
        fs.writeFile('csvs/psql_book.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('It\'s saved!');
            }
        });
    });
    stringify(psql_auth, function (err, output) {
        fs.writeFile('csvs/psql_author.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('It\'s saved!');
            }
        });
    });
    stringify(psql_city, function (err, output) {
        fs.writeFile('csvs/psql_city.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('It\'s saved!');
            }
        });
    });
    stringify(psql_mens, function (err, output) {
        fs.writeFile('csvs/psql_mention.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('It\'s saved!');
            }
        });
    });
    stringify(auth_book, function (err, output) {
        fs.writeFile('csvs/neo4j_auth_book.csv', output, 'utf8', function (err) {
            if (err) {
                console.log('Some error occured - file either not saved or corrupted file saved.');
            } else {
                console.log('It\'s saved!');
            }
        });
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
        successFunc_meta(id, filename, auth, title, release);

    }
    else if (filename == "baleng2.txt") {
        title = "Ancient Poems, Ballads and Songs of the Peasantry of England";
        auth = "Robert Bell";
        release = "1846";
        id = filename
        successFunc_meta(id, filename, auth, title, release);
    }
    else if (filename == "pntvw10.txt") {
        title = "The Point of View";
        auth = "Henry James";
        release = "01-10-2001";
        id = filename
        successFunc_meta(id, filename, auth, title, release);
    }
    else if (filename == "Introduction_and_Copyright.txt") {
        title = "The Common New Testament";
        auth = "Timothy Clontz";
        release = "14-03-1999";
        id = filename
        successFunc_meta(id, filename, auth, title, release);
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

                    //const process = fork('./cityscan.js');
                    //process.send({content})
                    cp.enqueue(content,(err,list)=>{
                        //console.log(list)
                        successFunc_city(book_id, list.stdout.city_list, (test) => {
                            console.log(count++,filename)
                            if (count == 50) {
                                console.timeEnd("dbsave")
                                writeToCsv();
                            }
                            //process.kill()
                            //console.log("thread kill: "+ threads.length)
                            //threads.pop()
                        })
                    })
                    /*process.on('message', (list)=>{
                        
                    })*/
                    /*else{
                        console.log("mother started")
                        scanCities_v2(content, (list) => {
                            successFunc_city(book_id, list, (test) => {
                                console.log(count++,filename)
                                if (count == 50) {
                                    console.timeEnd("dbsave")
                                    writeToCsv();
                                }
                            })
                        })
                    }*/
                });


            }


        })
    }
    /*
        for(var i = 0; i < list_ids.length; i++)
        {
            if(list_ids[i].id == id){
                id+=1000000
            }
          else if(list_ids[i].id == id){
            console.log("this id: 1: "+JSON.stringify({name:filename,id:id,dir:dirname}))
            console.log("that id: 2: "+JSON.stringify(list_ids[i]))
          }
          
        }
            list_ids.push({name:filename,id:id,dir:dirname})
    
        
        count_check++;
        if(count_check%10000 == 0){
            console.log(count_check)
        }*/
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


readFiles("files/10022/", somefunc, someErr)

