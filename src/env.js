/* eslint-disable no-whitespace-before-property */

import { assert }                   from 'fgc';
import _                            from 'lodash';

//----------------------------------------------------------------//
function getEnv ( name, fallback ) {
    const value = _.has ( process.env, name ) ? process.env [ name ] : fallback;
    assert ( value !== undefined, `Missing ${ name } environment variable.` );
    return value;
}

export const PORT                       = parseInt ( getEnv ( 'PORT', 7777 ), 10 );
export const SQLITE_FILE                = getEnv ( 'SQLITE_FILE' );
export const VOL_PRIMARY_URL            = getEnv ( 'VOL_PRIMARY_URL' );
