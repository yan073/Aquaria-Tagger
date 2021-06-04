#!/usr/bin/env node

import chalk from "chalk";
import clear from "clear";
import path from "path";
import mongodb from "mongodb";
import dotenv from "dotenv"

import {getAllRecordsUrl} from "./crawler";
import {queryArticles} from "./query";

const dboptions = { 
    useNewUrlParser: true,
    useUnifiedTopology: true, 
    connectTimeoutMS: 10000,
};

clear();
dotenv.config();

const dbUrl = process.env.MONGODB_URL;
 
console.log("- Connecting to " + process.env.MONGODB_URL );

const client = new mongodb.MongoClient(dbUrl, dboptions);

client.connect ( (err) => {
    if (err != null) {
        console.error( err );
    }
    else {
        console.log("\t opening database " + process.env.MONGODB_DATABASE );
        run( client.db(process.env.MONGODB_DATABASE) ).then( ()=> {
            console.log('- All done!');
            client.close();
        });
    }
});

async function run(db: mongodb.Db) {               
    const urls = await getAllRecordsUrl(process.env.NIHURL); 
    await queryArticles(urls, db); 
}
