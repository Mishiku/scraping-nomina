const parse = require('csv-parse')
const fs = require('fs')
const axios = require('axios').default;
var sleep = require('sleep');
const neatCsv = require('neat-csv');
global.lines = "nope";
// current timestamp in milliseconds
let ts = Date.now();

let date_ob = new Date(ts);
let date = date_ob.getDate();
let month = date_ob.getMonth() + 1;
let year = date_ob.getFullYear();

global.fecha = year + "-" + month + "-" + date;

const path = "NT_csvs_" + fecha;

if (!fs.existsSync(path)) {
    fs.mkdir("NT_csvs_" + fecha, (err) => {
        if (err) {
            throw err;
        }
        console.log("Directory is created.");
    });
}



async function write2json(response) {
    fs.appendFile('NT' + fecha + '.json', JSON.stringify(response), function (err) {
        if (err) return console.log(err);
    });

}

async function writecsv(response, r1, r2,name) {
    console.log("writing...");

    return await fs.writeFile("NT_csvs_" + fecha + '/' + r1 + '_' + r2 + '_'+name+'.csv', response, function (err) {
        if (err) return console.log(err);
    });

}

async function numberOfCalls(ramo, ur) {
    try {

        const owo = await axios({
            method: 'post',
            url: 'https://nomina-elastic.apps.funcionpublica.gob.mx/graphql/',
            timeout: 10000,
            data: { "operationName": "consultaNominaPorRamoTotal", "variables": { "_ramo": ramo, "_ur": ur }, "query": "query consultaNominaPorRamoTotal($_ramo: Int!, $_ur: String) {\n  consultaNominaPorRamoTotal(ramo: $_ramo, unidad: $_ur) {\n    responseCode\n    servPubTotalSector\n    __typename\n  }\n}\n" }
        });
        return owo.data['data']['consultaNominaPorRamoTotal']['servPubTotalSector'];

    } catch (error) {
        if (error.code == "ECONNREFUSED" || error.code == "EHOSTUNREACH") {
            console.log("reintentando #...");
            return numberOfCalls(ramo, ur);
        }
        console.log(error.code);
    }
}

async function apicall(ramo, ur) {

    try {
        // console.log("trying: " + ramo + " " + ur + " *************** ");

        const owo = await axios({
            method: 'post',
            url: 'https://nomina-elastic.apps.funcionpublica.gob.mx/graphql/',
            timeout: 10000,
            data: { "operationName": "consultaNominaPorRamoPaginado", "variables": { "_ramo": ramo, "_ur": ur, "_initial": 0, "_size": 10000 }, "query": "query consultaNominaPorRamoPaginado($_ramo: Int!, $_ur: String, $_initial: Int!, $_size: Int!) {\n  consultaNominaPorRamoPaginado(ramo: $_ramo, unidad: $_ur, registroInicio: $_initial, cantidadRegistros: $_size) {\n    responseCode\n    servPubTotalSector\n    listDtoServidorPublicoDto {\n      compensacionGarantizada\n      dependencia\n      nombrePuesto\n      nombres\n      primerApellido\n      segundoApellido\n      sueldoBase\n      ramo\n      idUr\n      __typename\n    }\n    __typename\n  }\n}\n" }
        });
        return owo.data['data']['consultaNominaPorRamoPaginado'].listDtoServidorPublicoDto;

    } catch (error) {
        if (error.code == "ECONNREFUSED" || error.code == "EHOSTUNREACH") {
            console.log("reintentando llamada...");
            return apicall(ramo, ur);
        }
        console.log(error.code);
    }

}
async function csvcall(ramo, ur) {

    try {
        console.log("trying csv: " + ramo + " " + ur + " *************** ");

        const owo = await axios({
            method: 'get',
            url: 'https://dgti-ejc-ftp.k8s.funcionpublica.gob.mx/APF/' + ramo + '_' + ur + '.csv',
            timeout: 120000
        });
        return owo.data;

    } catch (error) {
        if (error.code == "ECONNREFUSED" || error.code == "EHOSTUNREACH") {
            console.log("reintentando...");
            return csvcall(ramo, ur);
        }
        console.log(error.code);
    }

}


async function obtainSectors() {
    try {
        console.log("Obteniendo ramos...");

        const owo = await axios({
            method: 'get',
            url: 'https://nominatransparente.rhnet.gob.mx/assets/sectores.json',
            timeout: 5000
        });
        return owo.data;

    } catch (error) {
        console.log(error);
        return obtainSectors();
    }
}

async function obtainEntities(sect) {
    try {
        const owo = await axios({
            method: 'post',
            url: 'https://entes.apps.funcionpublica.gob.mx/',
            timeout: 10000,
            data: { "operationName": "obtenerEntes", "variables": { "ramo": sect }, "query": "query obtenerEntes($ramo: Int!) {\n  obtenerEntes(filtro: {ramo: $ramo, nivelGobierno: FEDERAL}) {\n    id\n    unidadResponsable\n    nombreCorto\n    enteDesc\n    __typename\n  }\n}\n" }
        });
        return owo.data['data']['obtenerEntes'];

    } catch (error) {
        console.log(error);
        return obtainEntities(sect);
    }

}

async function main() {
    const uwu = await obtainSectors();
    var indices = Object.keys(uwu);
    for (var i = 0; i < indices.length; i++) {
        const sect = uwu[indices[i]].id;
        console.log('\x1b[36m%s\x1b[0m', "RAMO: " + uwu[indices[i]].name);
        const ents = await obtainEntities(sect);

        var indexEnts = Object.keys(ents);
        for (var j = 0; j < indexEnts.length; j++) {
            const cycles = await numberOfCalls(sect, ents[indexEnts[j]].unidadResponsable);
            console.log(sect + " " + ents[indexEnts[j]].unidadResponsable + " " + ents[indexEnts[j]].nombreCorto + " " + cycles);
            if (cycles < 10000 && cycles > 0) {
                const callinfo = await apicall(sect, ents[indexEnts[j]].unidadResponsable);
                // console.log(callinfo[0]);
                const yay = await write2json(callinfo);
            }
            else {
                if (cycles > 0) {
                    const csvinfo = await csvcall(sect, ents[indexEnts[j]].unidadResponsable);
                    await writecsv(csvinfo, sect, ents[indexEnts[j]].unidadResponsable,ents[indexEnts[j]].nombreCorto);
                }
            }

        }
    }
}

main()
