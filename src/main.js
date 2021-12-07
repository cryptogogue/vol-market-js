/* eslint-disable no-whitespace-before-property */

process.on ( 'uncaughtException', function ( err ) {
    console.log ( err );
    process.exit ( 1 );
});

import * as env                     from 'env';
import { makeServer }               from 'server';

( async () => {
    const server = await makeServer ();
    await server.listen ( env.PORT );
    console.log ( 'LISTENING ON PORT:', env.PORT );
})();
