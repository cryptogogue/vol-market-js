/* eslint-disable no-whitespace-before-property */

import * as consts                  from 'consts';
import * as env                     from 'env';
import * as templates               from 'templates';
import bcrypt                       from 'bcrypt';
import crypto                       from 'crypto';
import _                            from 'lodash';
import fetch                        from 'node-fetch';
import { assert, token, util }      from 'fgc';
import handlebars                   from 'handlebars';
import Mailchimp                    from 'mailchimp-api-v3'; // https://mailchimp.com/developer/reference/
import nodemailer                   from 'nodemailer';
import secureRandom                 from 'secure-random';
import uuidv4                       from 'uuid/v4';

//================================================================//
// Mailer
//================================================================//
export class Mailer {

    //----------------------------------------------------------------//
    constructor () {

       this.mailchimp = new Mailchimp ( env.MAILCHIMP_API_KEY );

       this.mailTransport = nodemailer.createTransport ({
            service: 'gmail',
            auth: {
                user: env.GMAIL_USER,
                pass: env.GMAIL_PASSWORD,
            }
        });
    }

    //----------------------------------------------------------------//
    async mailchimpSubscribeAsync () {

        // const response = await mailchimp.post ( `/lists/${ env.MAILCHIMP_USER_LIST_ID }/members`, {
        //     email_address:      body.email,
        //     email_type:         'html',
        //     status:             'subscribed',
        //     merge_fields: {
        //         VERIFIER:   verifier,
        //     }
        // });
        // console.log ( 'SIGNUP:', JSON.stringify ( response, null, 4 ));
        // result.json ({});
    }

    //----------------------------------------------------------------//
    async sendVerifierEmailAsync ( email, redirect, subject, textTemplate, htmlTemplate, signingKey ) {
        
        const context = {
            verifier: token.create ( email, 'localhost', 'self', signingKey ),
            redirect: redirect || '/',
        };

        const text = textTemplate ( context );
        const html = htmlTemplate ( context );

        await this.mailTransport.sendMail ({
            from:       env.GMAIL_USER,
            to:         email,
            subject:    subject,
            text:       text,
            html:       html,
        });

        return context.verifier;
    }
}
