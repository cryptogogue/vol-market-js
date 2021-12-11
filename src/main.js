/* eslint-disable no-whitespace-before-property */

process.on ( 'uncaughtException', function ( err ) {
    console.log ( err );
    process.exit ( 1 );
});

import * as config                  from 'config';
import { makeServer }               from 'server';

( async () => {
    const server = await makeServer ();
    await server.listen ( config.PORT );
    console.log ( 'LISTENING ON PORT:', config.PORT );
})();
