const cities = require("all-the-cities");

async function scanCities(content) {
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
    return list
}
process.on('message', async (content) => {
    const list = await scanCities(content);
    process.send({ city_list: list })
})