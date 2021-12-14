// Copyright (c) 2019 Fall Guy LLC All Rights Reserved.

import { DB_CONTROL_COMMANDS }      from './VOLQueryDBSQLite';
import * as config                  from 'config';
import express                      from 'express';
import * as fgc                     from 'fgc';
import _                            from 'lodash';

const DB_COMMAND_REMAP = {
    reset:      DB_CONTROL_COMMANDS.RESET_INGEST,
};

//================================================================//
// VOLQueryREST
//================================================================//
export class VOLQueryREST {

    //----------------------------------------------------------------//
    constructor ( volQueryDB, ) {
        
        this.volQueryDB         = volQueryDB;

        this.router = express.Router ();

        this.router.post        ( '/commands/:command',             this.postCommand.bind ( this ));
        this.router.get         ( '/consensus',                     this.getConsensus.bind ( this ));
        this.router.get         ( '/offers',                        this.getOffers.bind ( this ));
        this.router.get         ( '/offers/:offerID',               this.getOffer.bind ( this ));
        this.router.get         ( '/stamps',                        this.getStamps.bind ( this ));
    }

    //----------------------------------------------------------------//
    async getConsensus ( request, response ) {

        console.log ( 'getConsensus' );

        try {
            const consensusService = volQueryDB.consensusService;
            const consensus = {
                height:             consensusService.height,
                digest:             consensusService.digest,
            };
            fgc.rest.handleSuccess ( response, consensus );
        }
        catch ( error ) {
            fgc.rest.handleError ( response, 400, error );
        }
    }

    //----------------------------------------------------------------//
    async getOffer ( request, response ) {

        try {
            const offerID = request.params.offerID;
            const offer = this.volQueryDB.getOffer ( offerID );
            fgc.rest.handleSuccess ( response, { offer: offer });
        }
        catch ( error ) {
            fgc.rest.handleError ( response, 400, error );
        }
    }

    //----------------------------------------------------------------//
    async getOffers ( request, response ) {

        const query             = request.query || {};

        const options = {
            all:                _.has ( query, 'all' ),
            base:               _.has ( query, 'base' ) ? parseInt ( query.base ) : 0,
            count:              _.has ( query, 'count' ) ? parseInt ( query.count ) : 20,
            excludeSeller:      query.exclude_seller || false,
            matchSeller:        query.match_seller|| false,
            token:              query.token || false,
        };

        try {
            const searchResult = this.volQueryDB.getOffers ( options );
            fgc.rest.handleSuccess ( response, searchResult );
        }
        catch ( error ) {
            fgc.rest.handleError ( response, 400, error );
        }
    }

    //----------------------------------------------------------------//
    async getStamps ( request, response ) {

        const query             = request.query || {};

        const options = {
            base:               _.has ( query, 'base' ) ? parseInt ( query.base ) : 0,
            count:              _.has ( query, 'count' ) ? parseInt ( query.count ) : 20,
            excludeSeller:      query.exclude_seller || false,
            matchSeller:        query.match_seller|| false,
            token:              query.token || false,
        };

        try {
            const searchResult = this.volQueryDB.getStamps ( options );
            fgc.rest.handleSuccess ( response, searchResult );
        }
        catch ( error ) {
            fgc.rest.handleError ( response, 400, error );
        }
    }

    //----------------------------------------------------------------//
    async postCommand ( request, response ) {

        try {
            
            const query = request.query || {};
            const commandKey = query.key;

            if ( config.DB_COMMAND_KEY !== commandKey ) {
                fgc.rest.handleError ( response, 401, 'Command key did not match.' );
                return;
            }

            const command = DB_COMMAND_REMAP [ request.params.command ];
            this.volQueryDB.pushControlCommand ( command );
            const commands = this.volQueryDB.getCommands ();
            fgc.rest.handleSuccess ( response, { commands: commands });
        }
        catch ( error ) {
            console.log ( error );
            fgc.rest.handleError ( response, 400, error );
        }
    }
}
