/* eslint-disable no-whitespace-before-property */

import { ERROR_STATUS }             from '../src/packages/fgc-common-js-src/ModelError';
import sqlite3                      from 'better-sqlite3';
import * as config                  from 'config';
import * as fgc                     from 'fgc';
import _                            from 'lodash';
import * as luxon                   from 'luxon';
import * as vol                     from 'vol';

const MAX_STEP_SIZE         = 10;
const FETCH_BLOCK_TIMEOUT   = 5000;
const FETCH_BATCH_SIZE      = 32;

export const DB_CONTROL_COMMANDS = {
    RESET_INGEST:       'RESET_INGEST',
};

//================================================================//
// VOLQueryDBSQLite
//================================================================//
export class VOLQueryDBSQLite {

    //----------------------------------------------------------------//
    affirmKnownOffer ( offer ) {

        // increment originNonce
        const nonces = this.getNonces ();
        nonces.origin++;

        const offerID = offer.offerID;
        this.affirmOffer ( offerID );

        this.db.prepare ( `
            UPDATE offers SET seller = ?, assets = ?, minimumPrice = ?, expiration = ?, originNonce = ? WHERE offerID = ?
        ` ).run ( offer.seller, JSON.stringify ( offer.assets ), offer.minimumPrice, offer.expiration, nonces.origin, offerID );

        for ( let asset of offer.assets ) {
            this.db.prepare ( `
                INSERT INTO offerAssets ( offerID, assetID, type ) VALUES ( ?, ?, ? )
            ` ).run ( offerID, asset.assetID, asset.type );
        }
        this.setNonces ( nonces );
    }

    //----------------------------------------------------------------//
    affirmOffer ( offerID ) {

        // create a new (empty) offer
        if ( !this.getOffer ( offerID )) {
            this.db.prepare ( `INSERT INTO offers ( offerID ) VALUES ( ? )` ).run ( offerID );
        }
    }

    //----------------------------------------------------------------//
    closeOffer ( offerID, closed ) {

        // increment closedNonce
        const nonces = this.getNonces ();
        nonces.closed++;

        this.affirmOffer ( offerID );
        this.db.prepare ( `UPDATE offers SET closed = ?, closedNonce = ? WHERE offerID = ?` ).run ( closed, nonces.closed, offerID );
        this.setNonces ( nonces );
    }

    //----------------------------------------------------------------//
    async fetchAccountAsync ( accountID, height ) {

        try {
            const accountURL = this.consensusService.getServiceURL ( `/accounts/${ accountID }`, { at: height });
            const result = await this.revocable.fetchJSON ( accountURL, undefined );
            if ( result && result.account ) {
                return result.account;
            }
        }
        catch ( error ) {
            console.log ( error );
        }
        delete fetching [ height ];
    }

    //----------------------------------------------------------------//
    constructor ( db ) {

        this.height             = 0;
        this.consensusService   = new vol.ConsensusService ();
        this.db                 = db || new sqlite3 ( config.SQLITE_FILE );
        this.revocable          = new fgc.RevocableContext ();

        this.db.prepare (`
            CREATE TABLE IF NOT EXISTS db_control (
                id              INTEGER         PRIMARY KEY AUTOINCREMENT,
                command         TEXT
            )
        `).run ();

        const command = this.getCommands ()[ 0 ];
        if ( command ) {
            console.log ( 'DB COMMAND:', command );
            if ( command === DB_CONTROL_COMMANDS.RESET_INGEST ) {
                console.log ( 'RESETTING INGEST' );
                this.db.prepare ( `DROP INDEX IF EXISTS offerAssets_type` ).run ();
                this.db.prepare ( `DROP TABLE IF EXISTS offerAssets` ).run ();
                this.db.prepare ( `DROP TABLE IF EXISTS offers` ).run ();
                this.db.prepare ( `DROP TABLE IF EXISTS nonces` ).run ();
                this.db.prepare ( `UPDATE blocks SET ingested = FALSE WHERE ingested = TRUE` ).run ();
            }
            this.db.prepare ( `DELETE FROM db_control` ).run ();
        }

        this.db.prepare (`
            CREATE TABLE IF NOT EXISTS blocks (
                height          INTEGER         PRIMARY KEY,
                txCount         INTEGER         NOT NULL DEFAULT 0,
                block           TEXT,
                found           BOOLEAN         NOT NULL DEFAULT FALSE,
                ingested        BOOLEAN         NOT NULL DEFAULT FALSE
            )
        `).run ();

        this.db.prepare (`
            CREATE TABLE IF NOT EXISTS nonces (
                id              INTEGER         PRIMARY KEY AUTOINCREMENT,
                origin          INTEGER         NOT NULL DEFAULT 0,
                closed          INTEGER         NOT NULL DEFAULT 0
            )
        `).run ();

        this.db.prepare (`
            CREATE TABLE IF NOT EXISTS offers (
                offerID         INTEGER         PRIMARY KEY,
                seller          INTEGER,
                assets          TEXT,
                minimumPrice    INTEGER         NOT NULL DEFAULT 0,
                expiration      TEXT,
                originNonce     INTEGER         NOT NULL DEFAULT 0,
                closedNonce     INTEGER         NOT NULL DEFAULT 0,
                closed          TEXT
            )
        `).run ();

        this.db.prepare (`
            CREATE TABLE IF NOT EXISTS offerAssets (
                id              INTEGER         PRIMARY KEY AUTOINCREMENT,
                offerID         INTEGER         NOT NULL,
                assetID         TEXT            NOT NULL,
                type            TEXT            NOT NULL,

                FOREIGN KEY ( offerID ) REFERENCES offers ( offerID )
            )
        `).run ();

        this.db.prepare ( `CREATE INDEX IF NOT EXISTS offerAssets_type ON offerAssets ( type )` ).run ();

        ( async () => {

            console.log ( 'INITIALIZING CONSENSUS SERVICE' );
            await this.consensusService.initializeWithNodeURLAsync ( config.VOL_PRIMARY_URL );
            console.log ( 'STARTING CONSENSUS SERVICE LOOP' );
            await this.consensusService.startServiceLoopAsync ();
            console.log ( 'STARTED CONSENSUS AT HEIGHT:', this.consensusService.height );

            this.populateBlockSearches ();
            this.serviceLoopAsync ();
            this.ingestLoopAsync ();
        })();
    }

    //----------------------------------------------------------------//
    async fetchOfferAsync ( assetID, height ) {

        try {
            const offerURL = this.consensusService.getServiceURL ( `/offers/${ assetID }`, { at: height });
            console.log ( 'FETCHING OFFER:', offerURL );
            const result = await this.revocable.fetchJSON ( offerURL, undefined, FETCH_BLOCK_TIMEOUT );
            if ( result ) {
                console.log ( result );
                return result;
            }
        }
        catch ( error ) {
            console.log ( error );
        }
        return false;
    }

    //----------------------------------------------------------------//
    getCommands () {

        return this.db.prepare ( `SELECT * FROM db_control` ).all ().map (( row ) => { return row.command; });
    }

    //----------------------------------------------------------------//
    getNonces () {

        const row = this.db.prepare ( `SELECT * FROM nonces` ).get ();
        return row || { origin: 0, closed: 0 };
    }

    //----------------------------------------------------------------//
    getOffer ( offerID ) {

        const row = this.db.prepare ( `SELECT * FROM offers WHERE offerID = ?` ).get ( offerID );
        return row ? this.rowToOffer ( row ) : false;
    }

    //----------------------------------------------------------------//
    getOffers ( options ) {

        const excludeSeller     = options.excludeSeller || -1;
        const matchSeller       = options.matchSeller || -1;
        const all               = options.all ? 1 : 0;

        const result = {
            offers:         [],
        };  

        let baseUTC;    // baseUTC excludes expired offers
        let origin;     // *excludes* offers added after search begins
        let closed;     // *includes* offers closed after search begins

        // if we have a token, we're continuing a search; otherwise, it's a new search
        if ( options.token ) {

            // parse the token
            const token     = JSON.parse ( Buffer.from ( options.token, 'base64' ).toString ( 'utf8' ));
            baseUTC         = token [ 0 ];
            origin          = token [ 1 ];
            closed          = token [ 2 ];
        }
        else {

            // get the nonces at the time of the query
            const nonces = this.getNonces ();

            baseUTC         = luxon.DateTime.utc ().startOf ( 'second' ).toISO ({ suppressMilliseconds: true });
            origin          = nonces.origin;
            closed          = nonces.closed;

            // encode the token
            result.token    = Buffer.from ( JSON.stringify ([ baseUTC, origin, closed ]), 'utf8' ).toString ( 'base64' );

            // get the count
            const countRow = this.db.prepare (`
                SELECT COUNT ( * ) AS count
                FROM offers
                WHERE                   originNonce > 0
                    AND                 originNonce <= ?
                    AND                 ( ? OR closed IS NULL )
                    AND                 ( ? OR ? < expiration )
                    AND                 seller != ?
                    AND                 ( ? < 0 OR seller = ? )
            `).get (
                origin,
                all,
                all, baseUTC,
                excludeSeller,
                matchSeller,
                matchSeller
            );

            result.count = countRow.count;
        }

        const rows = this.db.prepare (`
            SELECT *
            FROM offers
            WHERE                   originNonce > 0
                AND                 originNonce <= ?
                AND                 ( ? OR closedNonce > ? OR closed IS NULL )
                AND                 ( ? OR ? < expiration )
                AND                 seller != ?
                AND                 ( ? < 0 OR seller = ? )
            LIMIT ?, ?
        `).all (
            origin,
            all, closed,
            all, baseUTC,
            excludeSeller,
            matchSeller,
            matchSeller,
            options.base || 0,
            options.count || 20
        );

        result.offers = rows.map (( row ) => { return this.rowToOffer ( row ); });
        return result;
    }

    //----------------------------------------------------------------//
    getOpenOffers () {

        const nowUTC = luxon.DateTime.utc ().startOf ( 'second' ).toISO ({ suppressMilliseconds: true });
        console.log ( nowUTC );
        const rows = this.db.prepare ( `SELECT * FROM offers WHERE known = TRUE AND closed IS NULL AND ? < expiration` ).all ( nowUTC )
        console.log ( 'RESULT:', rows.length );
        return rows.map (( row ) => { return this.rowToOffer ( row ); });
    }

    //----------------------------------------------------------------//
    getOpenOffers () {

        const nowUTC = luxon.DateTime.utc ().startOf ( 'second' ).toISO ({ suppressMilliseconds: true });
        console.log ( nowUTC );
        const rows = this.db.prepare ( `SELECT * FROM offers WHERE known = TRUE AND closed IS NULL AND ? < expiration` ).all ( nowUTC )
        console.log ( 'RESULT:', rows.length );
        return rows.map (( row ) => { return this.rowToOffer ( row ); });
    }

    //----------------------------------------------------------------//
    getSellerOffers ( sellerID ) {

        const rows = this.db.prepare ( `SELECT * FROM offers WHERE known = TRUE AND seller = ?` ).all ( sellerID )
        return rows.map (( row ) => { return this.rowToOffer ( row ); });
    }

    //----------------------------------------------------------------//
    async ingestBlockAsync ( block ) {

        const height = block.height;
        const blockBody = JSON.parse ( block.body );

        console.log ( 'INGEST BLOCK:', height );

        // const assetIDs = {};
        for ( let transaction of blockBody.transactions ) {

            const txBody = transaction.bodyIn || JSON.parse ( transaction.body );
            console.log ( `${ height }: ${ txBody.type }` );

            switch ( txBody.type ) {

                case 'BUY_ASSETS': {
                    this.closeOffer ( txBody.offerID, 'COMPLETED' );
                    break;
                }

                case 'CANCEL_OFFER': {
                    const offer = await this.fetchOfferAsync ( txBody.identifier, height - 1 );
                    fgc.assert ( offer );
                    this.closeOffer ( offer.offerID, 'CANCELLED' );
                    break;
                }

                case 'OFFER_ASSETS': {

                    const offer = await this.fetchOfferAsync ( txBody.assetIdentifiers [ 0 ], height );
                    fgc.assert ( offer );

                    const seller = await this.fetchAccountAsync ( offer.seller, height );
                    fgc.assert ( seller );

                    offer.seller = seller.index;

                    this.affirmKnownOffer ( offer );
                    console.log ( offer );
                    break;
                }

                case 'RUN_SCRIPT': {
                    // console.log ( '  RUN_SCRIPT:' );
                    // for ( let invocation of txBody.invocations ) {
                    //     for ( let assetID in invocation.assetParams ) {
                    //         assetIDs [ assetID ] = true;
                    //         console.log ( '    ', assetID );
                    //     }
                    // }
                    break;
                }
            }
        }
    }

    //----------------------------------------------------------------//
    async ingestLoopAsync () {

        console.log ( 'BEGIN INGEST LOOP' );

        const statement = this.db.prepare ( `SELECT * FROM blocks WHERE ingested = FALSE AND found = TRUE AND txCount > 0 ORDER BY height DESC` );

        for ( let row = statement.get (); row; row = statement.get ()) {

            try {
                const block = JSON.parse ( row.block );
                await this.ingestBlockAsync ( block );
                this.db.prepare ( `UPDATE blocks SET ingested = TRUE WHERE height = ?` ).run ( row.height );
            }
            catch ( error ) {
                console.log ( error );
                break;
            }
        }

        this.revocable.timeout (() => { this.serviceLoopAsync ()}, 5000 );
    }

    //----------------------------------------------------------------//
    populateBlockSearches () {

        const row = this.db.prepare ( `SELECT COUNT ( * ) AS count FROM blocks` ).get ();

        for ( let i = row.count; i < this.consensusService.height; ++i ) {
            this.db.prepare ( `INSERT INTO blocks ( height ) VALUES ( ? )` ).run ( i );
        }
    }

    //----------------------------------------------------------------//
    pushControlCommand ( command ) {

        fgc.assert ( Object.values ( DB_CONTROL_COMMANDS ).includes ( command ));
        this.db.prepare ( `INSERT INTO db_control ( command ) VALUES ( ? )` ).run ( command );
    }

    //----------------------------------------------------------------//
    rowToOffer ( row ) {

        const offer = {
            offerID:            row.offerID,
            sellerIndex:        row.seller,
            assets:             JSON.parse ( row.assets ),
            minimumPrice:       row.minimumPrice,
            expiration:         row.expiration,
        };

        if ( row.closed ) {
            offer.closed = row.closed;
        }
        return offer;
    }

    //----------------------------------------------------------------//
    async serviceLoopAsync () {

        const fetching  = {};

        const fetchBlockAsync = async ( height ) => {
            try {
                const blockURL = this.consensusService.getServiceURL ( `/blocks/${ height }` );
                const result = await this.revocable.fetchJSON ( blockURL, undefined, FETCH_BLOCK_TIMEOUT );
                if ( result && result.block ) {

                    const block = result.block;
                    const blockBody = JSON.parse ( block.body );
                    // console.log ( 'GOT BLOCK:', height, blockBody.transactions.length );

                    this.db.prepare (`
                        UPDATE blocks SET found = TRUE, block = ?, txCount = ? WHERE height = ?
                    `).run (
                        JSON.stringify ( block ),
                        blockBody.transactions.length,
                        height
                    );
                }
            }
            catch ( error ) {
                console.log ( error );
            }
            delete fetching [ height ];
        }

        do {

            this.populateBlockSearches ();

            const currentSearches = _.size ( fetching );
            if ( currentSearches < FETCH_BATCH_SIZE ) {
                const rows = this.db.prepare ( `SELECT height FROM blocks WHERE found = FALSE ORDER BY height DESC LIMIT ${ FETCH_BATCH_SIZE - currentSearches }` ).all ();
                for ( let row of rows ) {
                    fetchBlockAsync ( row.height );
                }
            }
            const promise = fetching [ Object.keys ( fetching )[ 0 ]];
            if ( !promise ) break;
            await promise;

        } while ( _.size ( fetching ))

        this.revocable.timeout (() => { this.serviceLoopAsync ()}, 5000 );
    }

    //----------------------------------------------------------------//
    setNonces ( nonces ) {

        if ( !( nonces.origin || nonces.closed )) return;

        if ( nonces.id ) {
            this.db.prepare (
                `UPDATE nonces SET origin = ?, closed = ? WHERE id = ?`
            ).run (
                nonces.origin,
                nonces.closed,
                nonces.id,
            );
        }
        else {
            this.db.prepare (
                `INSERT INTO nonces ( origin, closed ) VALUES ( ?, ? )`
            ).run (
                nonces.origin,
                nonces.closed
            );
            nonces.id = this.db.lastInsertRowId;
        }
    }
}
