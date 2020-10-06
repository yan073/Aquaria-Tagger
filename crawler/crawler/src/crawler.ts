import mongodb from "mongodb";
import axios from "axios";
import sxml from "sxml";

export const getAllRecordsUrl = async (url: string) => {
    console.log('\t start crawling ' + url);
    //let page = await fetchPage(url);
    const response = await axios.get(url);
    if(response.status != 200){
        console.log("Error occurred while fetching " + url);
        return;
    }
    if(!response.data){
        console.log("Invalid data Obj");
        return;
    }
    let xml: sxml.XML = new sxml.XML(response.data);
    let xmlList: sxml.XMLList = xml.get("study");
    console.log("- found " + xmlList.size() + " studies."); 
    let urls: string[] = [];
    for (let study of xmlList) {
        let raw = study.get("url").at(0).getValue();
        urls.push( transformUrl(raw) );
    } 
    return urls
}

// https://ClinicalTrials.gov/show/NCT04372602
// https://clinicaltrials.gov/ct2/show/record/NCT04372602
const transformUrl = (url: string) => {
    let index = url.indexOf('gov/show/');
    return url.substring(0, index).concat(
                'gov/ct2/show/record/'
            ).concat(url.substring(index + 'gov/show/'.length));
};

const isRecordInCollection = async (url: string, collection: mongodb.Collection<any>) => {
    return null != await collection.findOne( {url: url} );
}