//THIS FILE IS NOT ACCURATE TO THE DATA USED IN SEED.JS
//IT IS LEFT OVER TO GIVE AN EXAMPLE OF WHAT YOU CAN DO WITH THE DATA BUT WOULD NEED TO BE REWORKED

const { Client, Pool } = require('pg')
const fs = require('fs');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: '2034',
    database: 'fcc_data',
    max: 25,
    keepAlive: true,
})

var feetDistance = 1000;

const getFaaResults = async () => {
    const towerClient = await pool.connect();
    const towers = await towerClient.query(`SELECT agl, x, y, objectid FROM faa_locations
        WHERE x::decimal BETWEEN -124.79 AND -66.91
        AND y::decimal BETWEEN 24.41 AND 49.38
        AND agl::decimal > 200
        ORDER BY agl::decimal DESC`
    );
    await towerClient.release();
    console.log(towers.rowCount);
    fs.writeFileSync('faa_results.json', '[]');

    await towers.rows.forEach(async (tower) => {
        const antennaClient = await pool.connect();
        const closeAntennas = await antennaClient.query(`
            SELECT * FROM am_locations
            WHERE am_dom_status = 'L' AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM fm_locations
            WHERE fm_dom_status='LIC' AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM tv_locations
            WHERE tv_dom_status = 'LIC' AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM cell_micro_locations
            WHERE ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);
        await antennaClient.release();

        if (!closeAntennas.some(e => e.rowCount != 0)) {
            let decomData = JSON.parse(fs.readFileSync('faa_results.json'));

            decomData.push({ id: tower.objectid, height: tower.agl, latitude: tower.y.trim(), longitude: tower.x.trim() });
            fs.writeFileSync('faa_results.json', JSON.stringify(decomData));
        }
    })
}

const getAsrResults = async () => {
    const towerClient = await pool.connect();
    const towers = await towerClient.query(`SELECT DISTINCT tr.registration_number, to_char(dms2dd(latitude_degrees::decimal, latitude_minutes::decimal, latitude_seconds::decimal, latitude_direction), '999.000000000') AS latitude,
        to_char(dms2dd(longitude_degrees::decimal, longitude_minutes::decimal, longitude_seconds::decimal, longitude_direction), '999.000000000') AS longitude,
        overall_height_above_ground::decimal AS height
        FROM tower_registrations tr
        JOIN asr_locations tc ON tc.registration_number = tr.registration_number
        WHERE overall_height_above_ground::decimal >= 60
        AND status_code = 'C'
        AND tc.registration_number != '1290280' --two edge cases where date_constructed = null
        AND tc.registration_number != '1304177'
        AND tc.coordinate_type = 'T'
        ORDER BY overall_height_above_ground::decimal DESC`
    );
    await towerClient.release();
    console.log(towers.rowCount);
    fs.writeFileSync('asr_results.json', '[]');

    await towers.rows.forEach(async (tower) => {
        const antennaClient = await pool.connect();
        const closeAntennas = await antennaClient.query(`
            SELECT * FROM am_locations
            WHERE am_dom_status = 'L' AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM fm_locations
            WHERE fm_dom_status='LIC' AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM tv_locations
            WHERE tv_dom_status = 'LIC' AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM cell_micro_locations
            WHERE ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);
        await antennaClient.release();

        if (!closeAntennas.some(e => e.rowCount != 0)) {
            let decomData = JSON.parse(fs.readFileSync('asr_results.json'));

            decomData.push({ id: tower.registration_number, height: tower.height, latitude: tower.latitude.trim(), longitude: tower.longitude.trim() });
            fs.writeFileSync('asr_results.json', JSON.stringify(decomData));
        }
    })
}

const crossCheck = async () => {
    const asrResults = await pool.query(`SELECT id, height, latitude, longitude FROM asr_results ORDER BY height DESC`);
    console.log("ASR rows: " + asrResults.rowCount);
    const del = await pool.query(`DELETE FROM crosscheck_results`);
    await asrResults.rows.forEach(async (result) => {
        const closeTowers = await pool.query(`
            SELECT * FROM faa_results
            WHERE ST_Dwithin((SELECT location_point FROM asr_results WHERE id = '${result.id}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);

        if(closeTowers.rowCount != 0)
        {
            let decomData = JSON.parse(fs.readFileSync('crosscheck_results.json'));

            decomData.push({ id: result.id, height: result.height, latitude: result.latitude.trim(), longitude: result.longitude.trim() });
            fs.writeFileSync('crosscheck_results.json', JSON.stringify(decomData));

            /*const insert = await pool.query(`
                INSERT INTO crosscheck_results VALUES ('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude}, 
                ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, ${result.latitude} ),4326 ), 2877))
            `);*/
        }
    })
}

const kml = (fileName, outputFileName) => {
    let decomData = JSON.parse(fs.readFileSync(fileName + ".json"));
    fs.writeFileSync("outputFileName.kml", `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
    <Folder>`);
    decomData.forEach((result, i) => {
        var kml = `
        <Placemark>
            <name>${result.id}-${result.height}</name>
            <description></description>
            <Point>
            <coordinates>${result.coordinates.split(", ")[1]},${result.coordinates.split(", ")[0]},0</coordinates>
            </Point>
        </Placemark>`;
        fs.appendFileSync("outputFileName.kml", kml);
        if(i == decomData.length - 1)
        {
            fs.appendFileSync("outputFileName.kml", `
    </Folder>
</kml>`);
        }
    })

}

const sqlUploadResults = async () => {
    const delClient = await pool.connect();
    await delClient.query(`DELETE FROM asr_results`);
    await delClient.release();

    const delClient2 = await pool.connect();
    await delClient2.query(`DELETE FROM faa_results`);
    await delClient2.release();

    let faaResults = JSON.parse(fs.readFileSync('faa_results.json'));
    faaResults.forEach(async result => {
        const insert = await pool.query(`
            INSERT INTO faa_results VALUES('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude},  
            ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, 
                                   ${result.latitude}
                    ),4326 ), 2877))
        `);
    });

    let asrResults = JSON.parse(fs.readFileSync('asr_results.json'));
    asrResults.forEach(async result => {
        const insert = await pool.query(`
            INSERT INTO asr_results VALUES('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude},  
            ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, 
                                   ${result.latitude}
                    ),4326 ), 2877))
        `);
    });
}

const getSafeFaaResults = async () => {
    const towerClient = await pool.connect();
    const towers = await towerClient.query(`SELECT agl, x, y, objectid FROM faa_locations
        WHERE x::decimal BETWEEN -124.79 AND -66.91
        AND y::decimal BETWEEN 24.41 AND 49.38
        AND agl::decimal > 300
        ORDER BY agl::decimal DESC`
    );
    await towerClient.release();
    console.log(towers.rowCount);
    fs.writeFileSync('faa_safe_results.json', '[]');

    await towers.rows.forEach(async (tower) => {
        const antennaClient = await pool.connect();
        const closeAntennas = await antennaClient.query(`
            SELECT * FROM am_locations
            WHERE am_dom_status = 'L' AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM fm_locations
            WHERE (horiz_erp::decimal > 5 OR vert_erp::decimal > 5) AND fm_dom_status='LIC'
            AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM tv_locations
            WHERE tv_dom_status = 'LIC' AND ST_Dwithin((SELECT location_point FROM faa_locations WHERE objectid = '${tower.objectid}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);
        await antennaClient.release();

        if (!closeAntennas.some(e => e.rowCount != 0)) {
            let decomData = JSON.parse(fs.readFileSync('faa_safe_results.json'));

            decomData.push({ id: tower.objectid, height: tower.agl, latitude: tower.y.trim(), longitude: tower.x.trim() });
            fs.writeFileSync('faa_safe_results.json', JSON.stringify(decomData));
        }
    })
}

const getSafeAsrResults = async () => {
    const towerClient = await pool.connect();
    const towers = await towerClient.query(`SELECT DISTINCT tr.registration_number, to_char(dms2dd(latitude_degrees::decimal, latitude_minutes::decimal, latitude_seconds::decimal, latitude_direction), '999.000000000') AS latitude,
        to_char(dms2dd(longitude_degrees::decimal, longitude_minutes::decimal, longitude_seconds::decimal, longitude_direction), '999.000000000') AS longitude,
        overall_height_above_ground::decimal AS height
        FROM tower_registrations tr
        JOIN asr_locations tc ON tc.registration_number = tr.registration_number
        WHERE overall_height_above_ground::decimal >= 91.44
        AND status_code = 'C'
        AND tc.registration_number != '1290280' --two edge cases where date_constructed = null
        AND tc.registration_number != '1304177'
        AND tc.coordinate_type = 'T'
        ORDER BY overall_height_above_ground::decimal DESC`
    );
    await towerClient.release();
    console.log(towers.rowCount);
    fs.writeFileSync('asr_safe_results.json', '[]');

    await towers.rows.forEach(async (tower) => {
        const antennaClient = await pool.connect();
        const closeAntennas = await antennaClient.query(`
            SELECT * FROM am_locations
            WHERE am_dom_status = 'L' AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM fm_locations
            WHERE (horiz_erp::decimal > 5 OR vert_erp::decimal > 5) AND fm_dom_status='LIC'
            AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;

            SELECT * FROM tv_locations
            WHERE tv_dom_status = 'LIC' AND ST_Dwithin((SELECT location_point FROM asr_locations WHERE registration_number = '${tower.registration_number}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);
        await antennaClient.release();

        if (!closeAntennas.some(e => e.rowCount != 0)) {
            let decomData = JSON.parse(fs.readFileSync('asr_safe_results.json'));

            decomData.push({ id: tower.registration_number, height: tower.height, latitude: tower.latitude.trim(), longitude: tower.longitude.trim() });
            fs.writeFileSync('asr_safe_results.json', JSON.stringify(decomData));
        }
    })
}

const sqlUploadSafeResults = async () => {
    const delClient = await pool.connect();
    await delClient.query(`DELETE FROM asr_safe_results`);
    await delClient.release();

    const delClient2 = await pool.connect();
    await delClient2.query(`DELETE FROM faa_safe_results`);
    await delClient2.release();

    let faaResults = JSON.parse(fs.readFileSync('faa_safe_results.json'));
    faaResults.forEach(async result => {
        const insert = await pool.query(`
            INSERT INTO faa_safe_results VALUES('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude},  
            ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, 
                                   ${result.latitude}
                    ),4326 ), 2877))
        `);
    });

    let asrResults = JSON.parse(fs.readFileSync('asr_safe_results.json'));
    asrResults.forEach(async result => {
        const insert = await pool.query(`
            INSERT INTO asr_safe_results VALUES('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude},  
            ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, 
                                   ${result.latitude}
                    ),4326 ), 2877))
        `);
    });
}

const safeCrossCheck = async () => {
    const asrResults = await pool.query(`SELECT id, height, latitude, longitude FROM asr_safe_results ORDER BY height DESC`);
    console.log("ASR rows: " + asrResults.rowCount);
    const del = await pool.query(`DELETE FROM crosscheck_safe_results`);
    fs.writeFileSync('crosscheck_safe_results.json', '[]');
    await asrResults.rows.forEach(async (result) => {
        const closeTowers = await pool.query(`
            SELECT * FROM faa_safe_results
            WHERE ST_Dwithin((SELECT location_point FROM asr_safe_results WHERE id = '${result.id}' LIMIT 1), location_point, ${feetDistance})
            LIMIT 1;
        `);

        if (closeTowers.rowCount != 0) {
            let decomData = JSON.parse(fs.readFileSync('crosscheck_safe_results.json'));

            decomData.push({ id: result.id, height: result.height, latitude: result.latitude.trim(), longitude: result.longitude.trim() });
            fs.writeFileSync('crosscheck_safe_results.json', JSON.stringify(decomData));

            /*const insert = await pool.query(`
                INSERT INTO crosscheck_results VALUES ('${result.id}', ${result.height}, ${result.latitude}, ${result.longitude}, 
                ST_Transform(ST_SetSRID(ST_MakePoint(${result.longitude}, ${result.latitude} ),4326 ), 2877))
            `);*/
        }
    })
}


const run = async () => {
    await getSafeFaaResults();
    console.log("finished FAA");
    await getSafeAsrResults();
    console.log("finished ASR");
    await sqlUploadSafeResults();
    console.log("finished uploading");
    await safeCrossCheck();
    console.log("finished crosscheck");
}

run();


const runCode = async () => {

    client.connect()
    /*const towers = await client.query(`SELECT DISTINCT tr.registration_number, to_char(dms2dd(latitude_degrees::decimal, latitude_minutes::decimal, latitude_seconds::decimal, latitude_direction), '999.000000000') AS latitude,
        to_char(dms2dd(longitude_degrees::decimal, longitude_minutes::decimal, longitude_seconds::decimal, longitude_direction), '999.000000000') AS longitude,
        overall_height_above_ground::decimal AS height, latitude_degrees, latitude_minutes, latitude_seconds, longitude_degrees, longitude_minutes, longitude_seconds
        FROM tower_registrations tr
        JOIN tower_locations tc ON tc.registration_number = tr.registration_number
        WHERE overall_height_above_ground::decimal >= 60
        AND status_code = 'C'
        AND tc.registration_number != '1290280' --two edge cases where date_constructed = null
        AND tc.registration_number != '1304177'
        AND dms2dd(longitude_degrees::decimal, longitude_minutes::decimal, longitude_seconds::decimal, longitude_direction) BETWEEN -124.79 AND -66.91
AND dms2dd(latitude_degrees::decimal, latitude_minutes::decimal, latitude_seconds::decimal, latitude_direction) BETWEEN 24.41 AND 49.38
        AND tc.coordinate_type = 'T'
        ORDER BY overall_height_above_ground::decimal DESC`);
    console.log(towers.rowCount);*/

    const towers = await client.query(`SELECT agl, x, y, objectid FROM faa_locations
        WHERE x::decimal BETWEEN -124.79 AND -66.91
        AND y::decimal BETWEEN 24.41 AND 49.38
        ORDER BY agl::decimal DESC`);
    console.log(towers.rowCount);

    /*const antennas = await client.query(`SELECT * FROM antenna_locations
		WHERE call_sign IN (
		SELECT call_sign FROM antenna_locations al
		WHERE al.lat_degrees IS NOT NULL
		AND al.location_number = 1
		AND UPPER(al.structure_type) IN ('2TOWER', 'GTOWER', 'LTOWER', 'MAST', 'MTOWER', 'NTOWER')
	    )`);
    console.log(antennas.rowCount);*/

    await towers.rows.forEach(async (tower) => {
        /*const somewhatCloseAntennas = await client.query(`SELECT call_sign FROM antenna_locations al
            WHERE lat_degrees::decimal = ${tower.latitude_degrees} AND lat_minutes::decimal = ${tower.latitude_minutes} AND long_degrees::decimal = ${tower.longitude_degrees} AND long_minutes::decimal = ${tower.longitude_minutes}
            LIMIT 1`);*/

        // console.log((`SELECT call_sign FROM antenna_tower_locations al
        //     WHERE latitude BETWEEN ${tower.latitude - .0001} AND ${tower.latitude + .0001} AND longitude BETWEEN ${tower.longitude - .0001} AND ${tower.longitude + .0001}
        //     LIMIT 1`));

        // const closeAntennas = await client.query(`SELECT call_sign FROM antenna_locations_gis al
        //     WHERE 
        //     lat_degrees IS NOT NULL AND
        //     dms2dd(lat_degrees::decimal, lat_minutes::decimal, lat_seconds::decimal, lat_direction) BETWEEN ${+tower.latitude - .002} AND ${+tower.latitude + .002} AND dms2dd(long_degrees::decimal, long_minutes::decimal, long_seconds::decimal, long_direction) BETWEEN ${+tower.longitude - .002} AND ${+tower.longitude + .002}
        //     LIMIT 1`);


        const closeAntennas = await client.query(`
        
        SELECT * FROM am_locations_gis
                WHERE ST_Dwithin((SELECT point FROM faa_locations_gis WHERE objectid = '${tower.objectid}' LIMIT 1), point, 1000)
                LIMIT 1;

SELECT * FROM fm_locations_gis
                WHERE ST_Dwithin((SELECT point FROM faa_locations_gis WHERE objectid = '${tower.objectid}' LIMIT 1), point, 1000)
                LIMIT 1;

            SELECT * FROM tv_locations_gis
                WHERE ST_Dwithin((SELECT point FROM faa_locations_gis WHERE objectid = '${tower.objectid}' LIMIT 1), point, 1000)
                LIMIT 1;

         SELECT * FROM cell_micro_locations_gis
                        WHERE ST_Dwithin((SELECT point FROM faa_locations_gis WHERE objectid = '${tower.objectid}' LIMIT 1), point, 1000)
                        LIMIT 1;
        
        `);
        
        if(!closeAntennas.some(e => e.rowCount != 0)) {
            //let decomData = JSON.parse(fs.readFileSync('faaDecoms.json'));

            //console.log(`{ registration_number: "${tower.objectid}", height: ${tower.agl}, coordinates: "${tower.y.trim()}, ${tower.x.trim()}" }`)

            insertFaaResults(tower.objectid, tower.agl, tower.y, tower.x);

            //decomData.push({ registration_number: tower.objectid, height: tower.agl, coordinates: `${tower.y.trim()}, ${tower.x.trim()}` });
            //fs.writeFileSync('faaDecoms.json', JSON.stringify(decomData));
            }


        

        /*const closeAntennas = await client.query(`SELECT * FROM antenna_tower_locations al
            WHERE geodistance(dms2dd(al.lat_degrees, al.lat_minutes, al.lat_seconds, al.lat_direction),
			dms2dd(long_degrees, long_minutes, long_seconds, long_direction),
			${tower.latitude},
			${tower.longitude}) < 0.0621371`);
        console.log(`towerNum: ${tower.registration_number} rowCount: ${closeAntennas.rows[0]}`);
        // console.log(closeAntennas.rows[0]);*/
    })
}

