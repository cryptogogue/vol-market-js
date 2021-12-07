/* eslint-disable no-whitespace-before-property */

import { VOLQueryDBSQLite }         from './VOLQueryDBSQLite';
import { VOLQueryREST }             from './VOLQueryREST';
import sqlite3                      from 'better-sqlite3';
import * as env                     from 'env';
import bodyParser                   from 'body-parser';
import express                      from 'express';
import * as fgc                     from 'fgc';
import * as vol                     from 'vol';

//----------------------------------------------------------------//
export async function makeServer ( db ) {

    db = db || new sqlite3 ( env.SQLITE_FILE );

    const consensusService = new vol.ConsensusService ();
    await consensusService.initializeWithNodeURLAsync ( env.VOL_PRIMARY_URL );
    await consensusService.startServiceLoopAsync ();
    console.log ( 'STARTED CONSENSUS AT HEIGHT:', consensusService.height );

    const server = express ();

    server.use ( function ( req, res, next ) {
        res.header ( 'Access-Control-Allow-Origin', '*' );
        res.header ( 'Access-Control-Allow-Headers', 'Origin, X-Requested-With, X-Auth-Token, Authorization, Content-Type, Accept' );
        res.header ( 'Access-Control-Allow-Methods', 'PUT, POST, GET, DELETE, PATCH, OPTIONS' );
        next ();
    });

    server.use ( bodyParser.json ());
    server.use ( bodyParser.urlencoded ({ extended: true }));

    let router = express.Router ();

    router.use ( new VOLQueryREST ( consensusService, new VOLQueryDBSQLite ( db )).router );

    router.get ( '/', ( request, result ) => {
        const message = {
            type: 'VOL_QUERY',
        };
        result.json ( message );
    });

    server.use ( '/', router );

    return server;
}
