/* eslint-disable no-whitespace-before-property */

import { ERROR_STATUS }             from '../src/packages/fgc-common-js-src/ModelError';
import * as consts                  from 'consts';
import { assert, ModelError }       from 'fgc';
import _                            from 'lodash';

//================================================================//
// VOLQueryDBSQLite
//================================================================//
export class VOLQueryDBSQLite {

    //----------------------------------------------------------------//
    constructor ( db ) {

        this.db = db;
    }
}