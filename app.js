const parse = require('csv-parse')
const fs = require('fs')
const axios = require('axios').default;
var sleep = require('sleep');
const neatCsv = require('neat-csv');
global.lines = "nope";


async function write2json(response) {
    fs.appendFile('datillos.json', JSON.stringify(response), function (err) {
        if (err) return console.log(err);
        // console.log(reponse);

        //console.log(response.data['data']['consultaNominaPorRamoPaginado']);
    });

}

async function writecsv(response, r1, r2) {
    console.log("writing...");

    await fs.writeFile('csvsOut/' + r1 + '_' + r2 + '.csv', response, function (err) {
        if (err) return console.log(err);
        // console.log(reponse);

        //console.log(response.data['data']['consultaNominaPorRamoPaginado']);
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
        console.error("reintentando #...");
        return await numberOfCalls(ramo, ur);
    }
}

async function apicall(ramo, ur) {

    try {
        console.log("trying: " + ramo + " " + ur + " *************** ");

        const owo = await axios({
            method: 'post',
            url: 'https://nomina-elastic.apps.funcionpublica.gob.mx/graphql/',
            timeout: 10000,
            data: { "operationName": "consultaNominaPorRamoPaginado", "variables": { "_ramo": ramo, "_ur": ur, "_initial": 0, "_size": 10000 }, "query": "query consultaNominaPorRamoPaginado($_ramo: Int!, $_ur: String, $_initial: Int!, $_size: Int!) {\n  consultaNominaPorRamoPaginado(ramo: $_ramo, unidad: $_ur, registroInicio: $_initial, cantidadRegistros: $_size) {\n    responseCode\n    servPubTotalSector\n    listDtoServidorPublicoDto {\n      compensacionGarantizada\n      dependencia\n      nombrePuesto\n      nombres\n      primerApellido\n      segundoApellido\n      sueldoBase\n      ramo\n      idUr\n      __typename\n    }\n    __typename\n  }\n}\n" }
        });
        return owo.data['data']['consultaNominaPorRamoPaginado'].listDtoServidorPublicoDto[0];

    } catch (error) {
        console.error("reintentando datos...");
        return await apicall(ramo, ur);
    }

}
async function csvcall(ramo, ur) {

    try {
        console.log("trying csv: " + ramo + " " + ur + " *************** ");

        const owo = await axios({
            method: 'get',
            url: 'https://dgti-ejc-ftp.k8s.funcionpublica.gob.mx/APF/' + ramo + '_' + ur + '.csv',
            timeout: 600000
        });
        return owo.data;

    } catch (error) {
        console.error("reintentando datos...");
        return await csvcall(ramo, ur);
    }

}
async function checa(r1, r2) {
    console.log("contando...");

    const readline = require('readline');
    const fs = require('fs');

    var file = 'csvsOut/' + r1 + '_' + r2 + '.csv';
    var linesCount = 0;
    var rl = fs.createReadStream(file);
    // rl.on('line', function(line){
    //         console.log("contando...");
            
    //         rl.pause();
    //         linesCount++; // on each linebreak, add +1 to 'linesCount'
    //         console.log(linesCount);

        
    // });
    // rl.on('close', async => {
    //     console.log(linesCount);
    //     if (linesCount > 10) {
    //         lines = "pass";
    //     }
        var rl = readline.createInterface({
            input: await fs.createReadStream(file),
            output: process.stdout,
            terminal: false
        });
        await rl.on('line', async (line)=> {
            try{
                rl.pause();
                linesCount++;
            }
            finally{
                rl.resume();
            }
            linesCount++; // on each linebreak, add +1 to 'linesCount'
        });
        rl.on('close', function () {
            console.log(linesCount);
            if(linesCount>10)
            {
                lines="pass";
            }
        });
    
}


async function watefoc() {
    var stream = await fs.createReadStream("ramos.csv")
        .pipe(parse({ delimiter: '~' }))
        .on('data', async (r) => {
            try {
                stream.pause();
                //sleep.sleep(5);
                const cycles = await numberOfCalls(r[0], r[1]);
                console.log(r[2] + ": " + cycles);
                if (cycles < 10000) {
                    const uwu = await apicall(r[0], r[1]);
                    const yay = await write2json(uwu);
                }
                else {
                    const exec = require("child_process").execSync;
                    lines = "nope";
                    var todobien = 0;
                    while (todobien == 0) {
                        switch (lines) {
                            case "nope":
                                // some code
                                console.log(lines);

                                const uwu = await csvcall(r[0], r[1]);
                                const yay = await writecsv(uwu, r[0], r[1]);
                                await new Promise(r => setTimeout(r, 1000));
                                const yny = await checa(r[0], r[1]);
                                await new Promise(r => setTimeout(r, 1000));
                                
                                break;
                            default:
                                console.log("lines = " + lines);
                                console.log(typeof lines);


                                todobien = 1;
                                break;
                        }
                    }
                }
            } finally {
                stream.resume();
            }
        })
}
watefoc()
// dame()
