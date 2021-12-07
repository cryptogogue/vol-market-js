// Copyright (c) 2019 Fall Guy LLC All Rights Reserved.

import express                      from 'express';
import * as fgc                     from 'fgc';
import _                            from 'lodash';

//================================================================//
// VOLQueryREST
//================================================================//
export class VOLQueryREST {

    //----------------------------------------------------------------//
    constructor ( consensusService, volQueryDB, ) {
        
        this.consensusService   = consensusService;
        this.volQueryDB         = volQueryDB;

        this.router = express.Router ();

        this.router.get         ( '/consensus',         this.getConsensus.bind ( this ));
    }

    //----------------------------------------------------------------//
    async getConsensus ( request, response ) {

        try {
            const consensus = {
                height:             this.consensusService.height,
                digest:             this.consensusService.digest,
            };
            fgc.rest.handleSuccess ( response, consensus );
        }
        catch ( error ) {
            fgc.rest.handleError ( response, error );
        }
    }
}
